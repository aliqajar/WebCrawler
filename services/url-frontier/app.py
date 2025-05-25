"""
URL Frontier Service

This service manages the URL frontier for the web crawler system.
It handles:
1. URL normalization and validation
2. Deduplication using Redis + PostgreSQL
3. Priority-based queuing
4. Communication with other services via Kafka

Architecture:
- Redis: Fast in-memory deduplication and caching
- PostgreSQL: Persistent URL storage and metadata
- Kafka: Message passing with other services
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import aioredis
import asyncpg
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from url_normalizer import URLNormalizer
from deduplicator import URLDeduplicator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class URLFrontier:
    def __init__(self):
        # Configuration from environment
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.db_url = os.getenv('DATABASE_URL', 'postgresql://crawler:crawler123@localhost:5432/webcrawler')
        
        # Components
        self.normalizer = URLNormalizer()
        self.deduplicator = None
        self.producer = None
        self.redis = None
        self.db_pool = None
        
        # Frontier settings
        self.batch_size = 100
        self.max_queue_size = 10000
        self.priority_levels = {
            'high': 200,
            'medium': 100,
            'low': 50
        }

    async def initialize(self):
        """Initialize all connections and components"""
        logger.info("Initializing URL Frontier service...")
        
        try:
            # Initialize Redis connection
            self.redis = await aioredis.from_url(self.redis_url)
            logger.info("Connected to Redis")
            
            # Initialize PostgreSQL connection pool
            self.db_pool = await asyncpg.create_pool(
                self.db_url,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Connected to PostgreSQL")
            
            # Initialize Kafka producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=3,
                acks='all'
            )
            logger.info("Connected to Kafka")
            
            # Initialize deduplicator
            self.deduplicator = URLDeduplicator(self.redis, self.db_pool)
            await self.deduplicator.initialize_bloom_filter()
            logger.info("Initialized deduplicator")
            
            logger.info("URL Frontier service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize URL Frontier: {e}")
            raise

    async def process_urls(self, urls: List[Dict], source: str = "unknown") -> Dict:
        """
        Process a batch of URLs through the frontier pipeline
        
        Args:
            urls: List of URL dictionaries with metadata
            source: Source of the URLs (seed, parser, etc.)
            
        Returns:
            Processing statistics
        """
        stats = {
            'total_received': len(urls),
            'normalized': 0,
            'duplicates_exact': 0,
            'duplicates_fuzzy': 0,
            'new_urls': 0,
            'queued_for_scheduling': 0,
            'errors': 0
        }
        
        new_urls = []
        
        for url_data in urls:
            try:
                # Extract URL and metadata
                raw_url = url_data.get('url', '')
                depth = url_data.get('depth', 0)
                source_priority = url_data.get('priority', 0)
                source_url = url_data.get('source_url', '')
                
                # Normalize URL
                normalized_url = self.normalizer.normalize(raw_url)
                if not normalized_url:
                    stats['errors'] += 1
                    continue
                    
                stats['normalized'] += 1
                
                # Check for duplicates
                dup_result = await self.deduplicator.check_duplicate(normalized_url)
                
                if dup_result['is_duplicate']:
                    if dup_result['match_type'] == 'exact':
                        stats['duplicates_exact'] += 1
                    else:
                        stats['duplicates_fuzzy'] += 1
                    
                    logger.debug(f"Duplicate URL found: {normalized_url} "
                               f"(type: {dup_result['match_type']}, "
                               f"score: {dup_result['similarity_score']})")
                    continue
                
                # Calculate priority
                priority = self.normalizer.get_url_priority(
                    normalized_url, depth, source_priority
                )
                
                # Extract domain
                domain = self.normalizer.extract_domain(normalized_url)
                if not domain:
                    stats['errors'] += 1
                    continue
                
                # Prepare URL record
                url_record = {
                    'url': raw_url,
                    'normalized_url': normalized_url,
                    'url_hash': self.deduplicator._generate_url_hash(normalized_url),
                    'domain': domain,
                    'priority': priority,
                    'depth': depth,
                    'source_url': source_url,
                    'source': source,
                    'created_at': datetime.now().isoformat()
                }
                
                new_urls.append(url_record)
                stats['new_urls'] += 1
                
            except Exception as e:
                logger.error(f"Error processing URL {url_data}: {e}")
                stats['errors'] += 1
        
        # Store new URLs in database
        if new_urls:
            stored_count = await self._store_urls_batch(new_urls)
            
            # Queue URLs for scheduling
            queued_count = await self._queue_urls_for_scheduling(new_urls[:stored_count])
            stats['queued_for_scheduling'] = queued_count
            
            logger.info(f"Processed {len(urls)} URLs: "
                       f"{stats['new_urls']} new, "
                       f"{stats['duplicates_exact']} exact duplicates, "
                       f"{stats['duplicates_fuzzy']} fuzzy duplicates")
        
        return stats

    async def _store_urls_batch(self, urls: List[Dict]) -> int:
        """Store URLs in PostgreSQL database"""
        if not urls:
            return 0
            
        try:
            async with self.db_pool.acquire() as conn:
                # Prepare data for batch insert
                url_data = [
                    (
                        url['url'],
                        url['normalized_url'],
                        url['url_hash'],
                        url['domain'],
                        url['priority'],
                        url['depth'],
                        url['source_url']
                    )
                    for url in urls
                ]
                
                # Batch insert URLs
                inserted = await conn.executemany("""
                    INSERT INTO urls (url, normalized_url, url_hash, domain, priority, depth, source_url)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (url_hash) DO NOTHING
                    RETURNING id
                """, url_data)
                
                # Add URLs to priority queue
                queue_data = []
                for i, url in enumerate(urls):
                    if i < len(inserted):  # Only queue successfully inserted URLs
                        queue_data.append((
                            inserted[i]['id'],
                            url['priority'],
                            url['domain']
                        ))
                
                if queue_data:
                    await conn.executemany("""
                        INSERT INTO url_queue (url_id, priority, domain)
                        VALUES ($1, $2, $3)
                    """, queue_data)
                
                # Update deduplication cache
                for url in urls:
                    await self.deduplicator.add_url_to_dedup_cache(
                        url['normalized_url'], 
                        url['url_hash']
                    )
                
                logger.info(f"Stored {len(inserted)} URLs in database")
                return len(inserted)
                
        except Exception as e:
            logger.error(f"Error storing URLs batch: {e}")
            return 0

    async def _queue_urls_for_scheduling(self, urls: List[Dict]) -> int:
        """Send URLs to scheduler via Kafka"""
        queued_count = 0
        
        for url in urls:
            try:
                # Prepare message for scheduler
                scheduler_message = {
                    'url': url['url'],
                    'normalized_url': url['normalized_url'],
                    'domain': url['domain'],
                    'priority': url['priority'],
                    'depth': url['depth'],
                    'queued_at': datetime.now().isoformat()
                }
                
                # Send to scheduler (partitioned by domain for load balancing)
                self.producer.send(
                    'url_scheduling',
                    value=scheduler_message,
                    key=url['domain']
                )
                
                queued_count += 1
                
            except KafkaError as e:
                logger.error(f"Failed to queue URL {url['url']}: {e}")
        
        # Ensure messages are sent
        self.producer.flush()
        
        logger.info(f"Queued {queued_count} URLs for scheduling")
        return queued_count

    async def get_priority_urls(self, limit: int = 100, domain: str = None) -> List[Dict]:
        """Get high-priority URLs from the queue"""
        try:
            async with self.db_pool.acquire() as conn:
                if domain:
                    # Get URLs for specific domain
                    query = """
                        SELECT u.url, u.normalized_url, u.domain, u.priority, u.depth
                        FROM url_queue q
                        JOIN urls u ON q.url_id = u.id
                        WHERE q.domain = $1 AND u.status = 'pending'
                        ORDER BY q.priority DESC, q.scheduled_for ASC
                        LIMIT $2
                    """
                    rows = await conn.fetch(query, domain, limit)
                else:
                    # Get URLs across all domains
                    query = """
                        SELECT u.url, u.normalized_url, u.domain, u.priority, u.depth
                        FROM url_queue q
                        JOIN urls u ON q.url_id = u.id
                        WHERE u.status = 'pending'
                        ORDER BY q.priority DESC, q.scheduled_for ASC
                        LIMIT $1
                    """
                    rows = await conn.fetch(query, limit)
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting priority URLs: {e}")
            return []

    async def consume_seed_urls(self):
        """Consume seed URLs from Kafka"""
        logger.info("Starting seed URL consumer...")
        
        consumer = KafkaConsumer(
            'seed_urls',
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='url_frontier_seeds',
            auto_offset_reset='latest'
        )
        
        try:
            for message in consumer:
                urls = message.value
                if not isinstance(urls, list):
                    urls = [urls]
                
                await self.process_urls(urls, source="seed")
                
        except Exception as e:
            logger.error(f"Error in seed URL consumer: {e}")
        finally:
            consumer.close()

    async def consume_discovered_urls(self):
        """Consume newly discovered URLs from parser"""
        logger.info("Starting discovered URL consumer...")
        
        consumer = KafkaConsumer(
            'discovered_urls',
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='url_frontier_discovered',
            auto_offset_reset='latest'
        )
        
        try:
            for message in consumer:
                urls = message.value
                if not isinstance(urls, list):
                    urls = [urls]
                
                await self.process_urls(urls, source="parser")
                
        except Exception as e:
            logger.error(f"Error in discovered URL consumer: {e}")
        finally:
            consumer.close()

    async def health_check(self):
        """Perform health checks and log statistics"""
        while True:
            try:
                # Get deduplication stats
                dedup_stats = await self.deduplicator.get_duplicate_stats()
                
                # Get queue stats
                async with self.db_pool.acquire() as conn:
                    pending_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM urls WHERE status = 'pending'"
                    )
                    total_count = await conn.fetchval("SELECT COUNT(*) FROM urls")
                    queue_size = await conn.fetchval("SELECT COUNT(*) FROM url_queue")
                
                logger.info(f"Frontier Stats - "
                           f"Total URLs: {total_count}, "
                           f"Pending: {pending_count}, "
                           f"Queue Size: {queue_size}, "
                           f"Bloom Filter: {dedup_stats.get('bloom_filter_size', 0)}")
                
                # Clean up caches periodically
                await self.deduplicator.cleanup_caches()
                
            except Exception as e:
                logger.error(f"Error in health check: {e}")
            
            await asyncio.sleep(60)  # Check every minute

    async def run(self):
        """Run the URL Frontier service"""
        logger.info("Starting URL Frontier service...")
        
        # Start all consumers and health check
        await asyncio.gather(
            self.consume_seed_urls(),
            self.consume_discovered_urls(),
            self.health_check()
        )


async def main():
    """Main entry point"""
    frontier = URLFrontier()
    
    try:
        await frontier.initialize()
        await frontier.run()
    except KeyboardInterrupt:
        logger.info("Shutting down URL Frontier service...")
    except Exception as e:
        logger.error(f"Fatal error in URL Frontier: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main()) 