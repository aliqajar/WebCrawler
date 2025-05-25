#!/usr/bin/env python3
"""
Test script for the Parser Service
Tests link extraction, content processing, and Kafka integration
"""

import asyncio
import json
import time
import random
from datetime import datetime
import redis.asyncio as redis
from kafka import KafkaProducer, KafkaConsumer

# Import our parser components
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'services', 'parser'))

from app import ParserService, LinkExtractor, ContentExtractor, ContentAnalyzer


class ParserTest:
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
    
    def get_test_html(self) -> str:
        """Get sample HTML for testing"""
        return """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="description" content="This is a comprehensive test page for web crawler parser testing with various content types and link structures.">
            <meta name="keywords" content="test, parser, web crawler, html, content extraction">
            <meta name="author" content="Test Author">
            <title>Comprehensive Test Page for Web Crawler Parser</title>
            <link rel="canonical" href="https://example.com/test-page">
            <link rel="alternate" href="https://example.com/test-page-mobile" media="handheld">
        </head>
        <body>
            <header>
                <nav>
                    <a href="/home">Home</a>
                    <a href="/about">About</a>
                    <a href="/contact">Contact</a>
                </nav>
            </header>
            
            <main>
                <article>
                    <h1>Main Article Title</h1>
                    <p>This is the first paragraph of our test article. It contains meaningful content that should be extracted by the parser. The content discusses various aspects of web crawling and content extraction techniques.</p>
                    
                    <p>The second paragraph continues with more detailed information about parsing algorithms. It mentions several important concepts like <a href="/concepts/dom-parsing">DOM parsing</a>, <a href="/concepts/text-extraction">text extraction</a>, and <a href="/concepts/link-analysis">link analysis</a>.</p>
                    
                    <h2>Subsection: Technical Details</h2>
                    <p>This section provides technical details about the implementation. It includes references to external resources like <a href="https://www.w3.org/TR/html5/" rel="external">HTML5 specification</a> and <a href="https://developer.mozilla.org/en-US/docs/Web/HTML" rel="external">MDN documentation</a>.</p>
                    
                    <ul>
                        <li><a href="/features/feature1">Feature 1: Advanced parsing</a></li>
                        <li><a href="/features/feature2">Feature 2: Content analysis</a></li>
                        <li><a href="/features/feature3">Feature 3: Link extraction</a></li>
                    </ul>
                    
                    <h3>Code Examples</h3>
                    <p>Here are some code examples that demonstrate the parsing capabilities:</p>
                    <pre><code>
                    def extract_content(html):
                        soup = BeautifulSoup(html, 'lxml')
                        return soup.get_text()
                    </code></pre>
                    
                    <h2>Images and Media</h2>
                    <p>The page also contains various media elements:</p>
                    <img src="/images/diagram1.png" alt="Parsing diagram">
                    <img src="/images/flowchart.jpg" alt="Process flowchart">
                    
                    <h2>External References</h2>
                    <p>Some external links that should be filtered:</p>
                    <a href="https://facebook.com/share">Share on Facebook</a>
                    <a href="https://twitter.com/intent/tweet">Tweet this</a>
                    <a href="https://linkedin.com/sharing">LinkedIn</a>
                    
                    <h2>File Downloads</h2>
                    <p>Links to files that should be excluded:</p>
                    <a href="/documents/manual.pdf">PDF Manual</a>
                    <a href="/downloads/software.zip">Download Software</a>
                    <a href="/media/video.mp4">Watch Video</a>
                </article>
                
                <aside>
                    <h3>Related Articles</h3>
                    <ul>
                        <li><a href="/related/article1">Related Article 1</a></li>
                        <li><a href="/related/article2">Related Article 2</a></li>
                        <li><a href="/related/article3">Related Article 3</a></li>
                    </ul>
                </aside>
            </main>
            
            <footer>
                <p>Copyright 2024 Test Company. All rights reserved.</p>
                <a href="/privacy">Privacy Policy</a>
                <a href="/terms">Terms of Service</a>
            </footer>
            
            <script>
                // This script content should be removed
                console.log("This should not appear in extracted content");
            </script>
            
            <style>
                /* This CSS should be removed */
                body { font-family: Arial, sans-serif; }
            </style>
        </body>
        </html>
        """
    
    async def test_link_extractor(self):
        """Test link extraction functionality"""
        print("\n🔗 Testing Link Extractor")
        
        extractor = LinkExtractor()
        html = self.get_test_html()
        
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'lxml')
        base_url = 'https://example.com/test-page'
        
        links = extractor.extract_links(soup, base_url)
        
        print(f"  📊 Extracted {len(links)} links")
        
        # Analyze link types
        internal_links = [l for l in links if l['link_type'] == 'internal']
        external_links = [l for l in links if l['link_type'] == 'external']
        canonical_links = [l for l in links if l['link_type'] == 'canonical']
        
        print(f"    Internal links: {len(internal_links)}")
        print(f"    External links: {len(external_links)}")
        print(f"    Canonical links: {len(canonical_links)}")
        
        # Test specific expectations
        expected_internal = ['/home', '/about', '/contact', '/concepts/dom-parsing']
        found_internal_urls = [l['url'] for l in internal_links]
        
        print(f"  🔍 Link Analysis:")
        for expected in expected_internal:
            full_url = f"https://example.com{expected}"
            found = any(full_url in url for url in found_internal_urls)
            print(f"    {expected}: {'✅' if found else '❌'}")
        
        # Check that excluded links are filtered
        excluded_patterns = ['facebook.com', 'twitter.com', '.pdf', '.zip', '.mp4']
        all_urls = [l['url'] for l in links]
        excluded_found = any(pattern in url for url in all_urls for pattern in excluded_patterns)
        print(f"    Excluded links filtered: {'✅' if not excluded_found else '❌'}")
        
        return len(links) > 0
    
    async def test_content_extractor(self):
        """Test content extraction functionality"""
        print("\n📄 Testing Content Extractor")
        
        extractor = ContentExtractor()
        html = self.get_test_html()
        url = 'https://example.com/test-page'
        
        result = extractor.extract_content(html, url)
        
        content = result['content']
        word_count = result['word_count']
        content_length = result['content_length']
        
        print(f"  📊 Content Statistics:")
        print(f"    Content length: {content_length} characters")
        print(f"    Word count: {word_count} words")
        print(f"    Extraction methods: {list(result['extraction_methods'].keys())}")
        
        # Check content quality
        expected_phrases = [
            'Main Article Title',
            'web crawling',
            'parsing algorithms',
            'Technical Details'
        ]
        
        print(f"  🔍 Content Quality Check:")
        for phrase in expected_phrases:
            found = phrase in content
            print(f"    '{phrase}': {'✅' if found else '❌'}")
        
        # Check that unwanted content is removed
        unwanted_phrases = [
            'console.log',  # JavaScript
            'font-family',  # CSS
            'This script content should be removed'
        ]
        
        print(f"  🚫 Unwanted Content Removal:")
        for phrase in unwanted_phrases:
            not_found = phrase not in content
            print(f"    '{phrase}' removed: {'✅' if not_found else '❌'}")
        
        return len(content) > 500  # Expect substantial content
    
    async def test_content_analyzer(self):
        """Test content analysis functionality"""
        print("\n📈 Testing Content Analyzer")
        
        analyzer = ContentAnalyzer()
        html = self.get_test_html()
        
        # Extract content first
        extractor = ContentExtractor()
        content_result = extractor.extract_content(html, 'https://example.com/test-page')
        content = content_result['content']
        
        # Analyze content
        metadata = {
            'title': 'Comprehensive Test Page for Web Crawler Parser',
            'description': 'This is a comprehensive test page for web crawler parser testing'
        }
        
        analysis = analyzer.analyze_content(content, metadata)
        
        print(f"  📊 Content Analysis Results:")
        print(f"    Word count: {analysis['word_count']}")
        print(f"    Sentence count: {analysis['sentence_count']}")
        print(f"    Paragraph count: {analysis['paragraph_count']}")
        print(f"    Reading time: {analysis['reading_time_minutes']:.1f} minutes")
        print(f"    Detected language: {analysis['detected_language']}")
        print(f"    Quality score: {analysis['quality_score']:.1f}/100")
        print(f"    Content type: {analysis['content_type']}")
        print(f"    Keywords: {', '.join(analysis['keywords'][:5])}")
        
        # Validate analysis
        checks = [
            (analysis['word_count'] > 100, "Word count > 100"),
            (analysis['sentence_count'] > 10, "Sentence count > 10"),
            (analysis['quality_score'] > 50, "Quality score > 50"),
            (analysis['detected_language'] == 'en', "Language detected as English"),
            (len(analysis['keywords']) > 0, "Keywords extracted")
        ]
        
        print(f"  ✅ Analysis Validation:")
        for check, description in checks:
            print(f"    {description}: {'✅' if check else '❌'}")
        
        return all(check for check, _ in checks)
    
    async def test_parser_integration(self):
        """Test full parser service integration"""
        print("\n🔧 Testing Parser Service Integration")
        
        # Set environment variables for test
        os.environ['KAFKA_SERVERS'] = 'localhost:9092'
        os.environ['REDIS_URL'] = self.redis_url
        os.environ['PROCESSING_BATCH_SIZE'] = '5'
        os.environ['MIN_CONTENT_LENGTH'] = '100'
        
        try:
            # Initialize parser service
            parser = ParserService()
            await parser.initialize()
            print("  ✅ Parser service initialized")
            
            # Create test raw content data (simulating fetcher output)
            raw_content_data = {
                'url': 'https://example.com/test-page',
                'final_url': 'https://example.com/test-page',
                'crawl_id': f"test_{int(time.time())}",
                'content': self.get_test_html(),
                'content_hash': 'test_hash_123',
                'content_type': 'text/html',
                'content_length': len(self.get_test_html()),
                'encoding': 'utf-8',
                'status_code': 200,
                'metadata': {
                    'title': 'Test Page',
                    'description': 'Test description',
                    'links_count': 0
                },
                'fetched_at': datetime.now().isoformat(),
                'shard_id': 0
            }
            
            print(f"  Processing content for: {raw_content_data['url']}")
            result = await parser.process_content(raw_content_data)
            
            if result['success']:
                print("  ✅ Content processed successfully")
                
                # Check result structure
                content = result.get('content', {})
                links = result.get('links', [])
                metadata = result.get('metadata', {})
                
                print(f"    Content length: {content.get('content_length', 0)} characters")
                print(f"    Links extracted: {len(links)}")
                print(f"    Quality score: {metadata.get('quality_score', 0):.1f}")
                print(f"    Content type: {metadata.get('content_type', 'unknown')}")
                print(f"    Processing time: {result.get('processing_info', {}).get('processing_time', 0):.2f}s")
                
                # Validate result structure
                required_fields = ['content', 'links', 'metadata', 'processing_info']
                structure_valid = all(field in result for field in required_fields)
                print(f"    Result structure: {'✅' if structure_valid else '❌'}")
                
            else:
                print(f"  ❌ Content processing failed: {result.get('error')}")
            
            # Test status endpoint
            status = await parser.get_parser_status()
            print(f"  📊 Parser status: {status['status']}")
            print(f"    Pages processed: {status['statistics']['pages_processed']}")
            
            return result['success']
            
        except Exception as e:
            print(f"  ❌ Integration test failed: {e}")
            return False
    
    async def test_kafka_integration(self):
        """Test Kafka message processing"""
        print("\n📨 Testing Kafka Integration")
        
        # Send test raw content to Kafka
        test_content = {
            'url': 'https://example.com/kafka-test',
            'crawl_id': f"kafka_test_{int(time.time())}",
            'content': self.get_test_html(),
            'content_type': 'text/html',
            'metadata': {'title': 'Kafka Test Page'},
            'fetched_at': datetime.now().isoformat()
        }
        
        # Send message to raw content topic
        self.producer.send(
            'raw_content',
            value=test_content,
            key=test_content['crawl_id']
        )
        print(f"  📤 Sent raw content: {test_content['url']}")
        
        self.producer.flush()
        print("  ✅ Message sent to Kafka")
        
        # Set up consumer to check for results
        consumer = KafkaConsumer(
            'parsed_content',
            'discovered_urls',
            'parsing_completed',
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='test_parser_consumer',
            auto_offset_reset='latest',
            consumer_timeout_ms=15000  # 15 second timeout
        )
        
        received_messages = []
        print("  📥 Listening for results...")
        
        try:
            for message in consumer:
                topic = message.topic
                data = message.value
                received_messages.append((topic, data))
                print(f"    Received from {topic}: {data.get('url', 'N/A')}")
                
                if len(received_messages) >= 3:  # parsed_content + discovered_urls + parsing_completed
                    break
                    
        except Exception as e:
            print(f"    Consumer timeout or error: {e}")
        finally:
            consumer.close()
        
        print(f"  📊 Received {len(received_messages)} messages from Kafka")
        
        # Analyze received messages
        topics_received = set(topic for topic, _ in received_messages)
        expected_topics = {'parsed_content', 'discovered_urls', 'parsing_completed'}
        
        print(f"  📋 Topic Analysis:")
        for topic in expected_topics:
            received = topic in topics_received
            print(f"    {topic}: {'✅' if received else '❌'}")
        
        return len(received_messages) > 0
    
    async def run_performance_test(self, num_pages=20):
        """Run performance test with multiple pages"""
        print(f"\n⚡ Running Performance Test ({num_pages} pages)")
        
        # Initialize parser
        os.environ['PROCESSING_BATCH_SIZE'] = '10'
        parser = ParserService()
        await parser.initialize()
        
        # Generate test pages with varying content
        test_pages = []
        for i in range(num_pages):
            # Vary content length and complexity
            content_multiplier = random.randint(1, 3)
            base_html = self.get_test_html()
            
            # Add more content for some pages
            if content_multiplier > 1:
                extra_content = "<p>Additional paragraph content. " * (content_multiplier * 10) + "</p>"
                base_html = base_html.replace("</article>", extra_content + "</article>")
            
            test_pages.append({
                'url': f'https://example.com/perf-test-{i}',
                'crawl_id': f"perf_test_{int(time.time())}_{i}",
                'content': base_html,
                'content_type': 'text/html',
                'metadata': {'title': f'Performance Test Page {i}'},
                'fetched_at': datetime.now().isoformat()
            })
        
        # Process pages concurrently
        start_time = time.time()
        
        tasks = []
        for page_data in test_pages:
            task = parser.process_content(page_data)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        total_time = time.time() - start_time
        
        # Analyze results
        successful = sum(1 for r in results if isinstance(r, dict) and r.get('success', False))
        failed = len(results) - successful
        
        total_links = sum(len(r.get('links', [])) for r in results if isinstance(r, dict) and r.get('success', False))
        total_content_length = sum(r.get('content', {}).get('content_length', 0) for r in results if isinstance(r, dict) and r.get('success', False))
        
        print(f"  📊 Performance Results:")
        print(f"    Total pages: {num_pages}")
        print(f"    Successful: {successful}")
        print(f"    Failed: {failed}")
        print(f"    Total time: {total_time:.2f}s")
        print(f"    Throughput: {num_pages / total_time:.1f} pages/second")
        print(f"    Average time per page: {total_time / num_pages:.2f}s")
        print(f"    Total links extracted: {total_links}")
        print(f"    Total content processed: {total_content_length:,} characters")
        
        return {
            'total_pages': num_pages,
            'successful': successful,
            'failed': failed,
            'total_time': total_time,
            'throughput': num_pages / total_time,
            'total_links': total_links
        }
    
    async def run_all_tests(self):
        """Run comprehensive parser tests"""
        print("🧪 Starting Parser Service Tests")
        print("=" * 50)
        
        await self.initialize()
        
        # Run individual tests
        test_results = {}
        
        try:
            # Test link extractor
            link_result = await self.test_link_extractor()
            test_results['link_extractor'] = link_result
            
            # Test content extractor
            content_result = await self.test_content_extractor()
            test_results['content_extractor'] = content_result
            
            # Test content analyzer
            analyzer_result = await self.test_content_analyzer()
            test_results['content_analyzer'] = analyzer_result
            
            # Test integration
            integration_result = await self.test_parser_integration()
            test_results['integration'] = integration_result
            
            # Test Kafka integration
            kafka_result = await self.test_kafka_integration()
            test_results['kafka'] = kafka_result
            
            # Run performance test
            perf_result = await self.run_performance_test(15)
            test_results['performance'] = perf_result
            
        except Exception as e:
            print(f"❌ Test execution failed: {e}")
            return False
        
        # Summary
        print(f"\n🎯 Test Summary")
        print("=" * 50)
        
        print(f"Link Extractor: {'✅' if test_results.get('link_extractor') else '❌'}")
        print(f"Content Extractor: {'✅' if test_results.get('content_extractor') else '❌'}")
        print(f"Content Analyzer: {'✅' if test_results.get('content_analyzer') else '❌'}")
        print(f"Integration: {'✅' if test_results.get('integration') else '❌'}")
        print(f"Kafka Integration: {'✅' if test_results.get('kafka') else '❌'}")
        
        if 'performance' in test_results:
            perf = test_results['performance']
            print(f"Performance: {perf['throughput']:.1f} pages/sec "
                  f"({perf['successful']}/{perf['total_pages']} successful, "
                  f"{perf['total_links']} links)")
        
        print(f"\n✅ All tests completed!")
        
        # Clean up
        if self.redis:
            await self.redis.close()
        if self.producer:
            self.producer.close()
        
        return True


async def main():
    """Main test runner"""
    test = ParserTest()
    await test.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main()) 