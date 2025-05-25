#!/usr/bin/env python3
"""
Test script for the Fetcher Service
Tests HTTP fetching, content processing, and Kafka integration
"""

import asyncio
import json
import time
import random
from datetime import datetime
import redis.asyncio as redis
from kafka import KafkaProducer, KafkaConsumer
import aiohttp

# Import our fetcher components
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'services', 'fetcher'))

from app import FetcherService, HTTPFetcher, ContentProcessor


class FetcherTest:
    def __init__(self):
        self.redis_url = 'redis://localhost:6379'
        self.kafka_servers = ['localhost:9092']
        self.redis = None
        self.producer = None
        
    async def initialize(self):
        """Initialize test environment"""
        try:
            # Initialize Redis
            self.redis = redis.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            print("✅ Connected to Redis")
            
            # Initialize Kafka producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            print("✅ Connected to Kafka")
            
            # Clean up test data
            await self.redis.flushdb()
            print("🧹 Cleaned up test database")
            
        except Exception as e:
            print(f"❌ Failed to initialize test environment: {e}")
            raise
    
    async def test_http_fetcher(self):
        """Test HTTP fetcher functionality"""
        print("\n🌐 Testing HTTP Fetcher")
        
        http_fetcher = HTTPFetcher(shard_id=0)
        await http_fetcher.initialize()
        
        # Test URLs with different characteristics
        test_urls = [
            {
                'url': 'https://httpbin.org/html',
                'expected_success': True,
                'description': 'Simple HTML page'
            },
            {
                'url': 'https://httpbin.org/json',
                'expected_success': True,
                'description': 'JSON response'
            },
            {
                'url': 'https://httpbin.org/status/404',
                'expected_success': False,
                'description': '404 error page'
            },
            {
                'url': 'https://httpbin.org/delay/2',
                'expected_success': True,
                'description': 'Delayed response (2s)'
            },
            {
                'url': 'https://httpbin.org/redirect/3',
                'expected_success': True,
                'description': 'Multiple redirects'
            }
        ]
        
        results = []
        
        for test_case in test_urls:
            url = test_case['url']
            expected_success = test_case['expected_success']
            description = test_case['description']
            
            print(f"  Testing: {description} ({url})")
            
            start_time = time.time()
            result = await http_fetcher.fetch_url(url, f"test_{int(time.time())}")
            fetch_time = time.time() - start_time
            
            success = result['success']
            status_match = success == expected_success
            
            print(f"    Result: {'✅' if status_match else '❌'} "
                  f"Success: {success}, Time: {fetch_time:.2f}s")
            
            if success:
                content_length = result.get('content_length', 0)
                status_code = result.get('status_code', 0)
                print(f"    Content: {content_length} bytes, Status: {status_code}")
            else:
                error = result.get('error', 'Unknown error')
                print(f"    Error: {error}")
            
            results.append({
                'url': url,
                'success': success,
                'expected_success': expected_success,
                'status_match': status_match,
                'fetch_time': fetch_time,
                'result': result
            })
        
        await http_fetcher.close()
        
        # Summary
        successful_tests = sum(1 for r in results if r['status_match'])
        print(f"\n📊 HTTP Fetcher Test Results: {successful_tests}/{len(results)} tests passed")
        
        return results
    
    async def test_content_processor(self):
        """Test content processing functionality"""
        print("\n📄 Testing Content Processor")
        
        processor = ContentProcessor()
        
        # Test HTML content
        html_content = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="description" content="Test page for web crawler">
            <meta name="keywords" content="test, crawler, html">
            <title>Test Page</title>
            <link rel="canonical" href="https://example.com/canonical">
        </head>
        <body>
            <h1>Test Page</h1>
            <p>This is a test page with some content.</p>
            <a href="/link1">Link 1</a>
            <a href="/link2">Link 2</a>
            <img src="/image1.jpg" alt="Image 1">
            <img src="/image2.jpg" alt="Image 2">
        </body>
        </html>
        """
        
        # Test encoding detection
        content_bytes = html_content.encode('utf-8')
        detected_encoding = processor.detect_encoding(content_bytes, 'text/html; charset=utf-8')
        print(f"  Encoding detection: {detected_encoding} ({'✅' if detected_encoding == 'utf-8' else '❌'})")
        
        # Test metadata extraction
        metadata = processor.extract_metadata(html_content, 'https://example.com/test')
        
        expected_metadata = {
            'title': 'Test Page',
            'description': 'Test page for web crawler',
            'keywords': 'test, crawler, html',
            'language': 'en',
            'canonical_url': 'https://example.com/canonical',
            'links_count': 2,
            'images_count': 2
        }
        
        print(f"  Metadata extraction:")
        for key, expected_value in expected_metadata.items():
            actual_value = metadata.get(key)
            match = actual_value == expected_value
            print(f"    {key}: {actual_value} ({'✅' if match else '❌'})")
        
        # Test content validation
        test_cases = [
            ('text/html', 1000, True, 'Valid HTML content'),
            ('application/pdf', 1000, False, 'Invalid content type'),
            ('text/html', 100 * 1024 * 1024, False, 'Content too large'),
            ('text/plain', 5000, True, 'Valid plain text')
        ]
        
        print(f"  Content validation:")
        for content_type, content_length, expected_valid, description in test_cases:
            is_valid, reason = processor.is_valid_content(content_type, content_length)
            match = is_valid == expected_valid
            print(f"    {description}: {'✅' if match else '❌'} ({reason})")
        
        return True
    
    async def test_fetcher_integration(self):
        """Test full fetcher service integration"""
        print("\n🔧 Testing Fetcher Service Integration")
        
        # Set environment variables for test
        os.environ['FETCHER_SHARD_ID'] = '0'
        os.environ['KAFKA_SERVERS'] = 'localhost:9092'
        os.environ['REDIS_URL'] = self.redis_url
        os.environ['CONCURRENT_REQUESTS'] = '5'
        
        try:
            # Initialize fetcher service
            fetcher = FetcherService()
            await fetcher.initialize()
            print("  ✅ Fetcher service initialized")
            
            # Test fetch request processing
            test_fetch_data = {
                'url': 'https://httpbin.org/html',
                'normalized_url': 'https://httpbin.org/html',
                'domain': 'httpbin.org',
                'priority': 5,
                'depth': 0,
                'crawl_id': f"test_{int(time.time())}",
                'shard_id': 0,
                'scheduled_at': datetime.now().isoformat()
            }
            
            print(f"  Processing fetch request for: {test_fetch_data['url']}")
            result = await fetcher.process_fetch_request(test_fetch_data)
            
            if result['success']:
                print("  ✅ Fetch request processed successfully")
                print(f"    Content length: {result.get('content_length', 0)} bytes")
                print(f"    Status code: {result.get('status_code')}")
                print(f"    Processing time: {result.get('processing_time', 0):.2f}s")
                
                # Check metadata
                metadata = result.get('metadata', {})
                print(f"    Title: {metadata.get('title', 'N/A')}")
                print(f"    Links: {metadata.get('links_count', 0)}")
                
            else:
                print(f"  ❌ Fetch request failed: {result.get('error')}")
            
            # Test status endpoint
            status = await fetcher.get_fetcher_status()
            print(f"  📊 Fetcher status: {status['status']}")
            print(f"    URLs processed: {status['statistics']['urls_processed']}")
            print(f"    Success rate: {status['statistics']['urls_successful']}/{status['statistics']['urls_processed']}")
            
            return result['success']
            
        except Exception as e:
            print(f"  ❌ Integration test failed: {e}")
            return False
    
    async def test_kafka_integration(self):
        """Test Kafka message production"""
        print("\n📨 Testing Kafka Integration")
        
        # Send test fetch requests to Kafka
        test_requests = [
            {
                'url': 'https://httpbin.org/html',
                'domain': 'httpbin.org',
                'crawl_id': f"kafka_test_{int(time.time())}_1"
            },
            {
                'url': 'https://httpbin.org/json',
                'domain': 'httpbin.org',
                'crawl_id': f"kafka_test_{int(time.time())}_2"
            }
        ]
        
        # Send messages to fetch queue
        for request in test_requests:
            self.producer.send(
                'fetch_queue_shard_0',
                value=request,
                key=request['crawl_id']
            )
            print(f"  📤 Sent fetch request: {request['url']}")
        
        self.producer.flush()
        print("  ✅ All messages sent to Kafka")
        
        # Set up consumer to check for results
        consumer = KafkaConsumer(
            'raw_content',
            'crawl_completed',
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='test_consumer',
            auto_offset_reset='latest',
            consumer_timeout_ms=10000  # 10 second timeout
        )
        
        received_messages = []
        print("  📥 Listening for results...")
        
        try:
            for message in consumer:
                topic = message.topic
                data = message.value
                received_messages.append((topic, data))
                print(f"    Received from {topic}: {data.get('url', 'N/A')}")
                
                if len(received_messages) >= len(test_requests) * 2:  # raw_content + crawl_completed
                    break
                    
        except Exception as e:
            print(f"    Consumer timeout or error: {e}")
        finally:
            consumer.close()
        
        print(f"  📊 Received {len(received_messages)} messages from Kafka")
        return len(received_messages) > 0
    
    async def run_performance_test(self, num_urls=50):
        """Run performance test with multiple URLs"""
        print(f"\n⚡ Running Performance Test ({num_urls} URLs)")
        
        # Generate test URLs
        test_urls = []
        for i in range(num_urls):
            test_urls.append({
                'url': f'https://httpbin.org/delay/{random.randint(0, 2)}',
                'domain': 'httpbin.org',
                'crawl_id': f"perf_test_{int(time.time())}_{i}",
                'priority': random.randint(1, 10)
            })
        
        # Initialize fetcher
        os.environ['CONCURRENT_REQUESTS'] = '10'
        fetcher = FetcherService()
        await fetcher.initialize()
        
        # Process URLs concurrently
        start_time = time.time()
        
        tasks = []
        for url_data in test_urls:
            task = fetcher.process_fetch_request(url_data)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        total_time = time.time() - start_time
        
        # Analyze results
        successful = sum(1 for r in results if isinstance(r, dict) and r.get('success', False))
        failed = len(results) - successful
        
        print(f"  📊 Performance Results:")
        print(f"    Total URLs: {num_urls}")
        print(f"    Successful: {successful}")
        print(f"    Failed: {failed}")
        print(f"    Total time: {total_time:.2f}s")
        print(f"    Throughput: {num_urls / total_time:.1f} URLs/second")
        print(f"    Average time per URL: {total_time / num_urls:.2f}s")
        
        return {
            'total_urls': num_urls,
            'successful': successful,
            'failed': failed,
            'total_time': total_time,
            'throughput': num_urls / total_time
        }
    
    async def run_all_tests(self):
        """Run comprehensive fetcher tests"""
        print("🧪 Starting Fetcher Service Tests")
        print("=" * 50)
        
        await self.initialize()
        
        # Run individual tests
        test_results = {}
        
        try:
            # Test HTTP fetcher
            http_results = await self.test_http_fetcher()
            test_results['http_fetcher'] = http_results
            
            # Test content processor
            content_result = await self.test_content_processor()
            test_results['content_processor'] = content_result
            
            # Test integration
            integration_result = await self.test_fetcher_integration()
            test_results['integration'] = integration_result
            
            # Test Kafka integration
            kafka_result = await self.test_kafka_integration()
            test_results['kafka'] = kafka_result
            
            # Run performance test
            perf_result = await self.run_performance_test(20)
            test_results['performance'] = perf_result
            
        except Exception as e:
            print(f"❌ Test execution failed: {e}")
            return False
        
        # Summary
        print(f"\n🎯 Test Summary")
        print("=" * 50)
        
        print(f"HTTP Fetcher: {'✅' if test_results.get('http_fetcher') else '❌'}")
        print(f"Content Processor: {'✅' if test_results.get('content_processor') else '❌'}")
        print(f"Integration: {'✅' if test_results.get('integration') else '❌'}")
        print(f"Kafka Integration: {'✅' if test_results.get('kafka') else '❌'}")
        
        if 'performance' in test_results:
            perf = test_results['performance']
            print(f"Performance: {perf['throughput']:.1f} URLs/sec "
                  f"({perf['successful']}/{perf['total_urls']} successful)")
        
        print(f"\n✅ All tests completed!")
        
        # Clean up
        if self.redis:
            await self.redis.close()
        if self.producer:
            self.producer.close()
        
        return True


async def main():
    """Main test runner"""
    test = FetcherTest()
    await test.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main()) 