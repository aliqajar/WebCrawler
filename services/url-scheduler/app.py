"""
URL Scheduler Service

This service sits between the URL Frontier and Fetchers, applying:
1. Politeness rules (robots.txt, crawl delays, rate limiting)
2. Domain sharding (load balancing across fetchers)
3. Priority-based scheduling
4. Adaptive scheduling based on server responses

Architecture Flow:
URL Frontier → URL Scheduler → Fetcher Shards
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

import aioredis
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import redis.asyncio as redis

from politeness_manager import PolitenessManager
from domain_sharding import DomainSharding

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BucketedDelayQueue:
    """
    High-performance delay queue using time-bucketed Redis lists
    Provides O(1) insertions instead of O(log N) sorted sets
    """
    
    def __init__(self, redis_client, bucket_size_seconds=30):
        self.redis = redis_client
        self.bucket_size = bucket_size_seconds  # 30-second buckets
        self.queue_prefix = "delayed_queue_bucket"
        
    def get_bucket_key(self, timestamp: float) -> str:
        """Get Redis key for time bucket"""
        bucket_id = int(timestamp // self.bucket_size)
        return f"{self.queue_prefix}_{bucket_id}"
    
    async def schedule_delayed(self, url_data: Dict, delay_seconds: float, reason: str) -> None:
        """
        Schedule URL for delayed processing - O(1) operation
        
        Args:
            url_data: URL information
            delay_seconds: Delay in seconds
            reason: Reason for delay
        """
        try:
            future_time = time.time() + delay_seconds
            bucket_key = self.get_bucket_key(future_time)
            
            delayed_item = {
                'url_data': url_data,
                'ready_at': future_time,
                'reason': reason,
                'delay_seconds': delay_seconds,
                'attempts': url_data.get('scheduling_attempts', 0) + 1,
                'scheduled_at': time.time()
            }
            
            # O(1) list push operation
            await self.redis.lpush(bucket_key, json.dumps(delayed_item))
            
            # Set expiration for automatic cleanup (delay + 1 hour buffer)
            expiration_seconds = int(delay_seconds + 3600)
            await self.redis.expire(bucket_key, expiration_seconds)
            
            logger.debug(f"Scheduled URL {url_data.get('url')} in bucket {bucket_key} "
                        f"for {delay_seconds}s: {reason}")
            
        except Exception as e:
            logger.error(f"Error scheduling delayed URL: {e}")
    
    async def get_ready_urls(self, max_items: int = 100) -> List[Dict]:
        """
        Get URLs that are ready to be processed - O(1) per bucket
        
        Args:
            max_items: Maximum number of URLs to return
            
        Returns:
            List of ready URL items
        """
        ready_items = []
        current_time = time.time()
        current_bucket = int(current_time // self.bucket_size)
        
        try:
            # Check current bucket and a few past buckets to handle edge cases
            buckets_to_check = range(current_bucket - 2, current_bucket + 1)
            
            for bucket_id in buckets_to_check:
                if len(ready_items) >= max_items:
                    break
                    
                bucket_key = f"{self.queue_prefix}_{bucket_id}"
                
                # Get all items from bucket - O(1) operation
                items = await self.redis.lrange(bucket_key, 0, -1)
                
                if items:
                    # Process items in this bucket
                    bucket_ready_items = []
                    bucket_not_ready_items = []
                    
                    for item_data in items:
                        try:
                            delayed_item = json.loads(item_data)
                            if delayed_item['ready_at'] <= current_time:
                                bucket_ready_items.append(delayed_item)
                            else:
                                bucket_not_ready_items.append(item_data)
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON in delayed queue: {item_data}")
                    
                    # If we found ready items, update the bucket
                    if bucket_ready_items:
                        ready_items.extend(bucket_ready_items[:max_items - len(ready_items)])
                        
                        # Update bucket: remove all items and re-add not-ready ones
                        await self.redis.delete(bucket_key)
                        if bucket_not_ready_items:
                            await self.redis.lpush(bucket_key, *bucket_not_ready_items)
                            # Reset expiration
                            await self.redis.expire(bucket_key, 3600)
            
            return ready_items
            
        except Exception as e:
            logger.error(f"Error getting ready URLs: {e}")
            return []
    
    async def get_queue_stats(self) -> Dict:
        """Get statistics about the delayed queue"""
        try:
            current_time = time.time()
            current_bucket = int(current_time // self.bucket_size)
            
            total_items = 0
            bucket_count = 0
            
            # Check buckets in a reasonable range
            for bucket_id in range(current_bucket - 10, current_bucket + 10):
                bucket_key = f"{self.queue_prefix}_{bucket_id}"
                bucket_size = await self.redis.llen(bucket_key)
                if bucket_size > 0:
                    total_items += bucket_size
                    bucket_count += 1
            
            return {
                'total_delayed_items': total_items,
                'active_buckets': bucket_count,
                'bucket_size_seconds': self.bucket_size,
                'current_bucket_id': current_bucket
            }
            
        except Exception as e:
            logger.error(f"Error getting queue stats: {e}")
            return {'total_delayed_items': 0, 'active_buckets': 0}


class URLScheduler:
    def __init__(self):
        # Configuration from environment
        self.kafka_servers = os.getenv('KAFKA_SERVERS', 'localhost:9092').split(',')
        self.redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.scheduling_batch_size = int(os.getenv('SCHEDULING_BATCH_SIZE', '50'))
        self.delayed_queue_check_interval = int(os.getenv('DELAYED_QUEUE_CHECK_INTERVAL', '30'))
        self.rebalance_interval = int(os.getenv('REBALANCE_INTERVAL', '300'))
        
        # Initialize components (will be set up in initialize())
        self.redis = None
        self.producer = None
        self.politeness_manager = None
        self.domain_sharding = None
        self.delay_queue = None  # Will be BucketedDelayQueue
        
        # Statistics
        self.stats = {
            'urls_scheduled': 0,
            'urls_delayed': 0,
            'urls_rejected': 0,
            'politeness_violations': 0,
            'shard_assignments': 0
        }
        
        logger.info("URL Scheduler initialized with bucketed delay queue")

    async def initialize(self):
        """Initialize all connections and components"""
        logger.info("Initializing URL Scheduler service...")
        
        try:
            # Initialize Redis connection
            self.redis = redis.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            logger.info("Connected to Redis")
            
            # Initialize bucketed delay queue after Redis connection
            self.delay_queue = BucketedDelayQueue(self.redis, bucket_size_seconds=30)
            logger.info("Initialized bucketed delay queue with 30-second buckets")
            
            # Initialize Kafka producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=3,
                acks='all'
            )
            logger.info("Connected to Kafka")
            
            # Initialize components
            self.politeness_manager = PolitenessManager(self.redis)
            await self.politeness_manager.initialize()
            
            self.domain_sharding = DomainSharding(self.redis)
            await self.domain_sharding.initialize()
            
            logger.info("URL Scheduler service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize URL Scheduler: {e}")
            raise

    async def schedule_url(self, url_data: Dict) -> Dict:
        """
        Schedule a single URL for fetching
        
        Args:
            url_data: URL information from frontier
            
        Returns:
            Scheduling result with status and details
        """
        url = url_data.get('url', '')
        domain = url_data.get('domain', '')
        priority = url_data.get('priority', 0)
        
        try:
            # Generate unique crawl ID
            crawl_id = f"{domain}:{uuid.uuid4().hex[:8]}:{int(datetime.now().timestamp())}"
            
            # Check politeness rules
            can_crawl, reason, delay = await self.politeness_manager.can_crawl_url(url)
            
            if not can_crawl:
                if delay > 0:
                    # Schedule for later using bucketed queue - O(1) operation
                    await self.delay_queue.schedule_delayed(url_data, delay, reason)
                    self.stats['urls_delayed'] += 1
                    return {
                        'status': 'delayed',
                        'reason': reason,
                        'delay_seconds': delay,
                        'crawl_id': crawl_id
                    }
                else:
                    # Permanently rejected
                    self.stats['urls_rejected'] += 1
                    self.stats['politeness_violations'] += 1
                    return {
                        'status': 'rejected',
                        'reason': reason,
                        'crawl_id': crawl_id
                    }
            
            # Assign to fetcher shard
            shard_id, shard_reason = await self.domain_sharding.assign_shard(domain, url)
            
            # Check if assigned shard is overloaded
            if await self.domain_sharding.is_shard_overloaded(shard_id):
                # Try to find alternative shard or delay
                alternative_shard = await self._find_alternative_shard(domain, url)
                if alternative_shard is not None:
                    shard_id = alternative_shard
                    shard_reason = "reassigned to alternative shard"
                else:
                    # All shards overloaded, delay the URL - O(1) operation
                    await self.delay_queue.schedule_delayed(url_data, 30, "all shards overloaded")
                    self.stats['urls_delayed'] += 1
                    return {
                        'status': 'delayed',
                        'reason': 'all shards overloaded',
                        'delay_seconds': 30,
                        'crawl_id': crawl_id
                    }
            
            # Register crawl start with politeness manager
            await self.politeness_manager.register_crawl_start(url, crawl_id)
            
            # Prepare message for fetcher
            fetch_message = {
                'url': url,
                'normalized_url': url_data.get('normalized_url', url),
                'domain': domain,
                'priority': priority,
                'depth': url_data.get('depth', 0),
                'crawl_id': crawl_id,
                'shard_id': shard_id,
                'scheduled_at': datetime.now().isoformat(),
                'scheduler_metadata': {
                    'shard_reason': shard_reason,
                    'politeness_check': reason
                }
            }
            
            # Send to appropriate fetcher shard
            queue_name = await self.domain_sharding.get_fetcher_queue_name(shard_id)
            self.producer.send(queue_name, value=fetch_message, key=domain)
            
            self.stats['urls_scheduled'] += 1
            self.stats['shard_assignments'] += 1
            
            logger.debug(f"Scheduled URL {url} to shard {shard_id} (crawl_id: {crawl_id})")
            
            return {
                'status': 'scheduled',
                'shard_id': shard_id,
                'queue_name': queue_name,
                'crawl_id': crawl_id,
                'reason': f"assigned to shard {shard_id}"
            }
            
        except Exception as e:
            logger.error(f"Error scheduling URL {url}: {e}")
            self.stats['urls_rejected'] += 1
            return {
                'status': 'error',
                'reason': f"scheduling error: {e}",
                'crawl_id': crawl_id if 'crawl_id' in locals() else 'unknown'
            }

    async def _find_alternative_shard(self, domain: str, url: str) -> Optional[int]:
        """Find an alternative shard that's not overloaded"""
        try:
            shard_stats = await self.domain_sharding.get_shard_statistics()
            current_loads = shard_stats.get('current_loads', {})
            
            # Find shard with lowest load
            min_load = float('inf')
            best_shard = None
            
            for shard_id in range(self.domain_sharding.num_fetcher_shards):
                load = current_loads.get(shard_id, 0)
                if load < min_load and not await self.domain_sharding.is_shard_overloaded(shard_id):
                    min_load = load
                    best_shard = shard_id
            
            return best_shard
            
        except Exception as e:
            logger.error(f"Error finding alternative shard: {e}")
            return None

    async def process_scheduling_requests(self):
        """Process URLs from the frontier that need scheduling"""
        logger.info("Starting scheduling request consumer...")
        
        consumer = KafkaConsumer(
            'url_scheduling',
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='url_scheduler',
            auto_offset_reset='latest',
            max_poll_records=self.scheduling_batch_size
        )
        
        try:
            for message in consumer:
                url_data = message.value
                
                # Process URL scheduling
                result = await self.schedule_url(url_data)
                
                # Log result for monitoring
                if result['status'] in ['rejected', 'error']:
                    logger.warning(f"Failed to schedule URL {url_data.get('url')}: {result['reason']}")
                
        except Exception as e:
            logger.error(f"Error in scheduling request consumer: {e}")
        finally:
            consumer.close()

    async def process_delayed_queue(self):
        """Process URLs that were delayed and are now ready"""
        logger.info("Starting delayed queue processor...")
        
        while True:
            try:
                current_time = datetime.now().timestamp()
                
                # Get URLs that are ready to be scheduled
                ready_items = await self.delay_queue.get_ready_urls(max_items=self.scheduling_batch_size)
                
                if ready_items:
                    # Process each ready URL
                    for item_data in ready_items:
                        try:
                            url_data = item_data['url_data']
                            
                            # Add scheduling attempt count
                            url_data['scheduling_attempts'] = item_data.get('attempts', 1)
                            
                            # Limit retry attempts
                            if url_data['scheduling_attempts'] > 5:
                                logger.warning(f"Dropping URL after 5 scheduling attempts: {url_data.get('url')}")
                                self.stats['urls_rejected'] += 1
                                continue
                            
                            # Try to schedule again
                            result = await self.schedule_url(url_data)
                            
                            logger.debug(f"Processed delayed URL {url_data.get('url')}: {result['status']}")
                            
                        except Exception as e:
                            logger.error(f"Error processing delayed item: {e}")
                
                # Wait before checking again
                await asyncio.sleep(self.delayed_queue_check_interval)
                
            except Exception as e:
                logger.error(f"Error in delayed queue processor: {e}")
                await asyncio.sleep(10)  # Wait longer on error

    async def handle_crawl_completions(self):
        """Handle notifications when crawls complete"""
        logger.info("Starting crawl completion handler...")
        
        consumer = KafkaConsumer(
            'crawl_completed',
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='scheduler_completion_handler',
            auto_offset_reset='latest'
        )
        
        try:
            for message in consumer:
                completion_data = message.value
                
                url = completion_data.get('url', '')
                crawl_id = completion_data.get('crawl_id', '')
                success = completion_data.get('success', False)
                response_time = completion_data.get('response_time', 0)
                
                # Notify politeness manager
                await self.politeness_manager.register_crawl_complete(
                    url, crawl_id, success, response_time
                )
                
                logger.debug(f"Processed crawl completion for {url} (success: {success})")
                
        except Exception as e:
            logger.error(f"Error in crawl completion handler: {e}")
        finally:
            consumer.close()

    async def periodic_maintenance(self):
        """Perform periodic maintenance tasks"""
        logger.info("Starting periodic maintenance...")
        
        while True:
            try:
                # Clean up expired data
                await self.politeness_manager.cleanup_expired_crawls()
                await self.domain_sharding.cleanup_old_assignments()
                
                # Rebalance shards if needed
                rebalance_result = await self.domain_sharding.rebalance_shards()
                if rebalance_result.get('rebalanced'):
                    logger.info(f"Rebalanced shards: moved {rebalance_result['domains_moved']} domains")
                
                # Log statistics
                await self._log_statistics()
                
                # Wait for next maintenance cycle
                await asyncio.sleep(self.rebalance_interval)
                
            except Exception as e:
                logger.error(f"Error in periodic maintenance: {e}")
                await asyncio.sleep(60)  # Wait shorter on error

    async def _log_statistics(self):
        """Log comprehensive scheduler statistics"""
        try:
            # Get shard statistics
            shard_stats = await self.domain_sharding.get_shard_statistics()
            
            # Get delayed queue statistics
            delay_queue_stats = await self.delay_queue.get_queue_stats()
            delayed_count = delay_queue_stats.get('total_delayed_items', 0)
            active_buckets = delay_queue_stats.get('active_buckets', 0)
            
            # Log comprehensive stats
            logger.info(f"Scheduler Stats - "
                       f"Scheduled: {self.stats['urls_scheduled']}, "
                       f"Delayed: {self.stats['urls_delayed']}, "
                       f"Rejected: {self.stats['urls_rejected']}, "
                       f"Delayed Queue: {delayed_count} URLs in {active_buckets} buckets, "
                       f"Active Shards: {shard_stats.get('total_shards', 0)}")
            
            # Log shard load distribution
            load_dist = shard_stats.get('load_distribution', {})
            if load_dist:
                load_info = ", ".join([f"Shard {sid}: {load:.1f}%" for sid, load in load_dist.items()])
                logger.info(f"Shard Load Distribution - {load_info}")
            
        except Exception as e:
            logger.error(f"Error logging statistics: {e}")

    async def get_scheduler_status(self) -> Dict:
        """Get comprehensive scheduler status"""
        try:
            # Get component statistics
            shard_stats = await self.domain_sharding.get_shard_statistics()
            delay_queue_stats = await self.delay_queue.get_queue_stats()
            
            return {
                'service': 'url_scheduler',
                'status': 'running',
                'statistics': self.stats.copy(),
                'delayed_queue': {
                    'total_items': delay_queue_stats.get('total_delayed_items', 0),
                    'active_buckets': delay_queue_stats.get('active_buckets', 0),
                    'bucket_size_seconds': delay_queue_stats.get('bucket_size_seconds', 30),
                    'current_bucket_id': delay_queue_stats.get('current_bucket_id', 0)
                },
                'shard_statistics': shard_stats,
                'configuration': {
                    'scheduling_batch_size': self.scheduling_batch_size,
                    'delayed_queue_check_interval': self.delayed_queue_check_interval,
                    'rebalance_interval': self.rebalance_interval,
                    'queue_type': 'bucketed_lists'
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting scheduler status: {e}")
            return {'service': 'url_scheduler', 'status': 'error', 'error': str(e)}

    async def run(self):
        """Run the URL Scheduler service"""
        logger.info("Starting URL Scheduler service...")
        
        # Start all tasks
        await asyncio.gather(
            self.process_scheduling_requests(),
            self.process_delayed_queue(),
            self.handle_crawl_completions(),
            self.periodic_maintenance()
        )


async def main():
    """Main entry point"""
    scheduler = URLScheduler()
    
    try:
        await scheduler.initialize()
        await scheduler.run()
    except KeyboardInterrupt:
        logger.info("Shutting down URL Scheduler service...")
    except Exception as e:
        logger.error(f"Fatal error in URL Scheduler: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main()) 