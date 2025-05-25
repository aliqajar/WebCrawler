"""
Test script for URL Scheduler

This script demonstrates:
1. Politeness rule enforcement
2. Domain sharding strategies
3. Delayed queue management
4. Shard load balancing
"""

import asyncio
import json
import time
from kafka import KafkaProducer
import aioredis
from datetime import datetime, timedelta

async def test_politeness_manager():
    """Test politeness management functionality"""
    print("=== Testing Politeness Manager ===")
    
    try:
        from services.url_scheduler.politeness_manager import PolitenessManager
        
        # Connect to Redis
        redis = await aioredis.from_url('redis://localhost:6379')
        politeness = PolitenessManager(redis)
        
        test_urls = [
            "https://example.com/page1",
            "https://example.com/page2",  # Same domain - should be delayed
            "https://httpbin.org/delay/1",
            "https://github.com/robots.txt",  # Test robots.txt
        ]
        
        for url in test_urls:
            can_crawl, reason, delay = await politeness.can_crawl_url(url)
            print(f"URL: {url}")
            print(f"Can crawl: {can_crawl}")
            print(f"Reason: {reason}")
            print(f"Delay: {delay}s")
            print("-" * 50)
            
            if can_crawl:
                # Simulate crawl start
                crawl_id = f"test_{int(time.time())}"
                await politeness.register_crawl_start(url, crawl_id)
                
                # Simulate crawl completion after 1 second
                await asyncio.sleep(1)
                await politeness.register_crawl_complete(url, crawl_id, True, 1.0)
        
        await redis.close()
        
    except ImportError as e:
        print(f"Could not import politeness manager: {e}")
        print("Make sure the scheduler service is available")

async def test_domain_sharding():
    """Test domain sharding functionality"""
    print("\n=== Testing Domain Sharding ===")
    
    try:
        from services.url_scheduler.domain_sharding import DomainSharding
        
        # Connect to Redis
        redis = await aioredis.from_url('redis://localhost:6379')
        sharding = DomainSharding(redis)
        
        test_domains = [
            "example.com",
            "github.com", 
            "stackoverflow.com",
            "reddit.com",
            "example.com",  # Duplicate to test sticky assignment
        ]
        
        print("Testing different sharding strategies:")
        
        strategies = ['load_balanced', 'domain_sticky', 'hash_based', 'round_robin']
        
        for strategy in strategies:
            print(f"\n--- {strategy.upper()} Strategy ---")
            
            for domain in test_domains:
                url = f"https://{domain}/test"
                shard_id, reason = await sharding.assign_shard(domain, url, strategy)
                print(f"Domain: {domain:20} → Shard: {shard_id} ({reason})")
        
        # Test shard statistics
        print("\n--- Shard Statistics ---")
        stats = await sharding.get_shard_statistics()
        print(json.dumps(stats, indent=2))
        
        # Test rebalancing
        print("\n--- Testing Rebalancing ---")
        rebalance_result = await sharding.rebalance_shards()
        print(json.dumps(rebalance_result, indent=2))
        
        await redis.close()
        
    except ImportError as e:
        print(f"Could not import domain sharding: {e}")
        print("Make sure the scheduler service is available")

def send_test_urls_to_scheduler():
    """Send test URLs to the scheduler via Kafka"""
    print("\n=== Sending Test URLs to Scheduler ===")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Test URLs with different characteristics
        test_urls = [
            {
                "url": "https://example.com",
                "normalized_url": "https://example.com",
                "domain": "example.com",
                "priority": 100,
                "depth": 0,
                "queued_at": datetime.now().isoformat()
            },
            {
                "url": "https://example.com/about",
                "normalized_url": "https://example.com/about",
                "domain": "example.com",
                "priority": 80,
                "depth": 1,
                "queued_at": datetime.now().isoformat()
            },
            {
                "url": "https://github.com",
                "normalized_url": "https://github.com",
                "domain": "github.com",
                "priority": 90,
                "depth": 0,
                "queued_at": datetime.now().isoformat()
            },
            {
                "url": "https://stackoverflow.com/questions",
                "normalized_url": "https://stackoverflow.com/questions",
                "domain": "stackoverflow.com",
                "priority": 70,
                "depth": 1,
                "queued_at": datetime.now().isoformat()
            },
            # Rapid-fire URLs to test rate limiting
            {
                "url": "https://example.com/page1",
                "normalized_url": "https://example.com/page1",
                "domain": "example.com",
                "priority": 60,
                "depth": 2,
                "queued_at": datetime.now().isoformat()
            },
            {
                "url": "https://example.com/page2",
                "normalized_url": "https://example.com/page2",
                "domain": "example.com",
                "priority": 60,
                "depth": 2,
                "queued_at": datetime.now().isoformat()
            },
            {
                "url": "https://example.com/page3",
                "normalized_url": "https://example.com/page3",
                "domain": "example.com",
                "priority": 60,
                "depth": 2,
                "queued_at": datetime.now().isoformat()
            }
        ]
        
        print(f"Sending {len(test_urls)} URLs to scheduler...")
        
        for i, url_data in enumerate(test_urls):
            producer.send('url_scheduling', value=url_data, key=url_data['domain'])
            print(f"Sent URL {i+1}: {url_data['url']}")
            
            # Add small delay between rapid-fire URLs to test rate limiting
            if i >= 4:  # Last 3 URLs
                time.sleep(0.1)
        
        producer.flush()
        producer.close()
        print("All test URLs sent to scheduler!")
        
    except Exception as e:
        print(f"Error sending URLs to scheduler: {e}")

async def monitor_scheduler_queues():
    """Monitor scheduler queues and statistics"""
    print("\n=== Monitoring Scheduler Queues ===")
    
    try:
        redis = await aioredis.from_url('redis://localhost:6379')
        
        # Monitor for 30 seconds
        for i in range(6):
            print(f"\n--- Check {i+1} ---")
            
            # Check delayed queue
            delayed_count = await redis.zcard('delayed_scheduling_queue')
            print(f"Delayed queue size: {delayed_count}")
            
            if delayed_count > 0:
                # Get some delayed items
                delayed_items = await redis.zrange('delayed_scheduling_queue', 0, 2, withscores=True)
                for item, score in delayed_items:
                    try:
                        item_data = json.loads(item)
                        scheduled_time = datetime.fromtimestamp(score)
                        print(f"  Delayed URL: {item_data['url_data']['url']}")
                        print(f"  Reason: {item_data['reason']}")
                        print(f"  Scheduled for: {scheduled_time}")
                    except Exception as e:
                        print(f"  Error parsing delayed item: {e}")
            
            # Check shard assignments
            shard_keys = await redis.keys('shard_assignments:*')
            print(f"Active shard assignments: {len(shard_keys)}")
            
            # Check rate limiting
            rate_keys = await redis.keys('rate_limit:*')
            print(f"Rate limit entries: {len(rate_keys)}")
            
            # Check domain delays
            delay_keys = await redis.keys('domain_delays:*')
            print(f"Domain delay entries: {len(delay_keys)}")
            
            await asyncio.sleep(5)
        
        await redis.close()
        
    except Exception as e:
        print(f"Error monitoring queues: {e}")

async def test_scheduler_integration():
    """Test full scheduler integration"""
    print("\n=== Testing Full Scheduler Integration ===")
    
    # Step 1: Test individual components
    await test_politeness_manager()
    await test_domain_sharding()
    
    # Step 2: Send URLs to scheduler
    send_test_urls_to_scheduler()
    
    # Step 3: Monitor processing
    print("\nWaiting for scheduler to process URLs...")
    await asyncio.sleep(2)
    
    # Step 4: Monitor queues
    await monitor_scheduler_queues()

def main():
    """Main test function"""
    print("Web Crawler URL Scheduler Test")
    print("=" * 50)
    
    print("\nThis test demonstrates:")
    print("1. Politeness rule enforcement")
    print("2. Domain sharding strategies") 
    print("3. Delayed queue management")
    print("4. Load balancing across shards")
    print("5. Real-time monitoring")
    
    print("\nPrerequisites:")
    print("- Redis running on localhost:6379")
    print("- Kafka running on localhost:9092")
    print("- URL Scheduler service running")
    
    try:
        # Run the integration test
        asyncio.run(test_scheduler_integration())
        
        print("\n" + "=" * 50)
        print("Scheduler test completed!")
        print("\nTo see the scheduler in action:")
        print("1. docker-compose up -d redis kafka zookeeper")
        print("2. docker-compose up url-scheduler")
        print("3. python test_scheduler.py")
        
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"\nTest failed with error: {e}")
        print("\nMake sure all services are running:")
        print("- docker-compose up -d redis kafka zookeeper")
        print("- docker-compose up url-scheduler")

if __name__ == "__main__":
    main() 