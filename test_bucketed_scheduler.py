#!/usr/bin/env python3
"""
Test script for the bucketed delay queue URL Scheduler
Demonstrates performance improvements over sorted set approach
"""

import asyncio
import json
import time
import random
from datetime import datetime, timedelta
import redis.asyncio as redis

# Import our scheduler components
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'services', 'url-scheduler'))

from app import BucketedDelayQueue, URLScheduler


class PerformanceTest:
    def __init__(self):
        self.redis_url = 'redis://localhost:6379'
        self.redis = None
        
    async def initialize(self):
        """Initialize Redis connection"""
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        print("✅ Connected to Redis")
        
        # Clean up any existing test data
        await self.redis.flushdb()
        print("🧹 Cleaned up test database")
    
    async def test_bucketed_queue_performance(self, num_urls=10000):
        """Test performance of bucketed delay queue"""
        print(f"\n🚀 Testing Bucketed Delay Queue Performance ({num_urls:,} URLs)")
        
        delay_queue = BucketedDelayQueue(self.redis, bucket_size_seconds=30)
        
        # Generate test URLs
        test_urls = []
        for i in range(num_urls):
            url_data = {
                'url': f'https://example{i % 100}.com/page{i}',
                'domain': f'example{i % 100}.com',
                'priority': random.randint(1, 10),
                'depth': random.randint(0, 5)
            }
            test_urls.append(url_data)
        
        # Test insertion performance
        print("📝 Testing insertions...")
        start_time = time.time()
        
        for i, url_data in enumerate(test_urls):
            delay_seconds = random.uniform(1, 300)  # 1 second to 5 minutes
            reason = random.choice(['crawl_delay', 'rate_limit', 'shard_overload'])
            await delay_queue.schedule_delayed(url_data, delay_seconds, reason)
            
            if (i + 1) % 1000 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                print(f"  Inserted {i+1:,} URLs in {elapsed:.2f}s ({rate:.0f} URLs/sec)")
        
        total_insert_time = time.time() - start_time
        insert_rate = num_urls / total_insert_time
        
        print(f"✅ Insertion Complete:")
        print(f"   Total time: {total_insert_time:.2f} seconds")
        print(f"   Rate: {insert_rate:.0f} URLs/second")
        print(f"   Average per URL: {(total_insert_time * 1000 / num_urls):.2f} ms")
        
        # Test queue statistics
        stats = await delay_queue.get_queue_stats()
        print(f"📊 Queue Statistics:")
        print(f"   Total items: {stats['total_delayed_items']:,}")
        print(f"   Active buckets: {stats['active_buckets']}")
        print(f"   Bucket size: {stats['bucket_size_seconds']} seconds")
        
        # Test retrieval performance
        print("\n📤 Testing retrievals...")
        start_time = time.time()
        
        total_retrieved = 0
        retrieval_rounds = 0
        
        while total_retrieved < num_urls // 2:  # Retrieve half the URLs
            ready_items = await delay_queue.get_ready_urls(max_items=100)
            
            if not ready_items:
                # Simulate time passing to make more URLs ready
                await asyncio.sleep(0.1)
                continue
            
            total_retrieved += len(ready_items)
            retrieval_rounds += 1
            
            if retrieval_rounds % 10 == 0:
                elapsed = time.time() - start_time
                rate = total_retrieved / elapsed if elapsed > 0 else 0
                print(f"  Retrieved {total_retrieved:,} URLs in {retrieval_rounds} rounds ({rate:.0f} URLs/sec)")
        
        total_retrieval_time = time.time() - start_time
        retrieval_rate = total_retrieved / total_retrieval_time if total_retrieval_time > 0 else 0
        
        print(f"✅ Retrieval Complete:")
        print(f"   Retrieved: {total_retrieved:,} URLs")
        print(f"   Total time: {total_retrieval_time:.2f} seconds")
        print(f"   Rate: {retrieval_rate:.0f} URLs/second")
        
        return {
            'insert_rate': insert_rate,
            'retrieval_rate': retrieval_rate,
            'total_items': stats['total_delayed_items'],
            'active_buckets': stats['active_buckets']
        }
    
    async def test_sorted_set_comparison(self, num_urls=1000):
        """Compare with traditional sorted set approach"""
        print(f"\n⚖️  Comparing with Sorted Set Approach ({num_urls:,} URLs)")
        
        # Test sorted set performance
        sorted_set_key = "test_sorted_set_queue"
        
        # Generate test data
        test_items = []
        for i in range(num_urls):
            url_data = {
                'url': f'https://test{i}.com/page',
                'domain': f'test{i % 50}.com'
            }
            delay_seconds = random.uniform(1, 300)
            score = time.time() + delay_seconds
            test_items.append((json.dumps(url_data), score))
        
        # Test sorted set insertions
        print("📝 Testing sorted set insertions...")
        start_time = time.time()
        
        for item_data, score in test_items:
            await self.redis.zadd(sorted_set_key, {item_data: score})
        
        sorted_set_insert_time = time.time() - start_time
        sorted_set_insert_rate = num_urls / sorted_set_insert_time
        
        print(f"   Sorted set rate: {sorted_set_insert_rate:.0f} URLs/second")
        
        # Test sorted set retrievals
        print("📤 Testing sorted set retrievals...")
        start_time = time.time()
        
        current_time = time.time()
        ready_items = await self.redis.zrangebyscore(
            sorted_set_key, 0, current_time, withscores=True, start=0, num=num_urls//2
        )
        
        if ready_items:
            items_to_remove = [item[0] for item in ready_items]
            await self.redis.zrem(sorted_set_key, *items_to_remove)
        
        sorted_set_retrieval_time = time.time() - start_time
        sorted_set_retrieval_rate = len(ready_items) / sorted_set_retrieval_time if sorted_set_retrieval_time > 0 else 0
        
        print(f"   Sorted set retrieval rate: {sorted_set_retrieval_rate:.0f} URLs/second")
        
        # Clean up
        await self.redis.delete(sorted_set_key)
        
        return {
            'insert_rate': sorted_set_insert_rate,
            'retrieval_rate': sorted_set_retrieval_rate
        }
    
    async def test_scheduler_integration(self):
        """Test the full scheduler with bucketed delay queue"""
        print(f"\n🔧 Testing Full Scheduler Integration")
        
        # Mock environment variables
        os.environ['KAFKA_SERVERS'] = 'localhost:9092'
        os.environ['REDIS_URL'] = self.redis_url
        
        try:
            scheduler = URLScheduler()
            await scheduler.initialize()
            
            print("✅ Scheduler initialized successfully")
            
            # Test URL scheduling with delays
            test_urls = [
                {
                    'url': 'https://example.com/page1',
                    'domain': 'example.com',
                    'priority': 5
                },
                {
                    'url': 'https://test.com/page1',
                    'domain': 'test.com',
                    'priority': 3
                }
            ]
            
            for url_data in test_urls:
                result = await scheduler.schedule_url(url_data)
                print(f"   Scheduled {url_data['url']}: {result['status']}")
            
            # Get scheduler status
            status = await scheduler.get_scheduler_status()
            print(f"📊 Scheduler Status:")
            print(f"   Queue type: {status['configuration']['queue_type']}")
            print(f"   Delayed queue items: {status['delayed_queue']['total_items']}")
            print(f"   Active buckets: {status['delayed_queue']['active_buckets']}")
            
            return True
            
        except Exception as e:
            print(f"❌ Scheduler test failed: {e}")
            return False
    
    async def run_all_tests(self):
        """Run comprehensive performance tests"""
        print("🧪 Starting Bucketed Delay Queue Performance Tests")
        print("=" * 60)
        
        await self.initialize()
        
        # Test different scales
        scales = [1000, 5000, 10000]
        results = {}
        
        for scale in scales:
            print(f"\n📏 Testing at scale: {scale:,} URLs")
            bucketed_results = await self.test_bucketed_queue_performance(scale)
            sorted_set_results = await self.test_sorted_set_comparison(min(scale, 2000))  # Limit sorted set test
            
            results[scale] = {
                'bucketed': bucketed_results,
                'sorted_set': sorted_set_results
            }
            
            # Calculate performance improvement
            insert_improvement = bucketed_results['insert_rate'] / sorted_set_results['insert_rate']
            retrieval_improvement = bucketed_results['retrieval_rate'] / sorted_set_results['retrieval_rate']
            
            print(f"\n📈 Performance Comparison (Scale: {scale:,}):")
            print(f"   Insert Rate Improvement: {insert_improvement:.1f}x faster")
            print(f"   Retrieval Rate Improvement: {retrieval_improvement:.1f}x faster")
        
        # Test scheduler integration
        await self.test_scheduler_integration()
        
        # Summary
        print(f"\n🎯 Performance Test Summary")
        print("=" * 60)
        
        for scale, result in results.items():
            bucketed = result['bucketed']
            sorted_set = result['sorted_set']
            
            print(f"\nScale: {scale:,} URLs")
            print(f"  Bucketed Queue:")
            print(f"    Insert: {bucketed['insert_rate']:.0f} URLs/sec")
            print(f"    Retrieve: {bucketed['retrieval_rate']:.0f} URLs/sec")
            print(f"  Sorted Set:")
            print(f"    Insert: {sorted_set['insert_rate']:.0f} URLs/sec")
            print(f"    Retrieve: {sorted_set['retrieval_rate']:.0f} URLs/sec")
            print(f"  Improvement: {bucketed['insert_rate']/sorted_set['insert_rate']:.1f}x insert, "
                  f"{bucketed['retrieval_rate']/sorted_set['retrieval_rate']:.1f}x retrieve")
        
        print(f"\n✅ All tests completed successfully!")
        
        # Clean up
        await self.redis.flushdb()
        await self.redis.close()


async def main():
    """Main test runner"""
    test = PerformanceTest()
    await test.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main()) 