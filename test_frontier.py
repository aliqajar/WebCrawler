"""
Test script for URL Frontier

This script demonstrates:
1. URL normalization
2. Redis + PostgreSQL deduplication
3. Priority-based queuing
4. Fuzzy matching capabilities
"""

import asyncio
import json
from kafka import KafkaProducer
import time

def test_url_normalization():
    """Test URL normalization functionality"""
    from services.url_frontier.url_normalizer import URLNormalizer
    
    normalizer = URLNormalizer()
    
    test_urls = [
        "http://example.com/path/",
        "HTTP://EXAMPLE.COM/PATH",
        "https://www.example.com/path?utm_source=google&id=123",
        "example.com/path/../other",
        "https://example.com:443/path",
        "https://example.com/path?b=2&a=1",
        "https://example.com/image.jpg",  # Should be filtered out
        "invalid-url",
        "https://example.com/news/article-1",
        "https://example.com/news/article-2"
    ]
    
    print("=== URL Normalization Test ===")
    for url in test_urls:
        normalized = normalizer.normalize(url)
        priority = normalizer.get_url_priority(url, depth=1) if normalized else 0
        domain = normalizer.extract_domain(url) if normalized else None
        
        print(f"Original:   {url}")
        print(f"Normalized: {normalized}")
        print(f"Priority:   {priority}")
        print(f"Domain:     {domain}")
        print("-" * 50)

def send_test_urls():
    """Send test URLs to Kafka for processing"""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Test seed URLs
    seed_urls = [
        {
            "url": "https://example.com",
            "priority": 100,
            "depth": 0
        },
        {
            "url": "https://news.ycombinator.com",
            "priority": 90,
            "depth": 0
        },
        {
            "url": "https://github.com",
            "priority": 85,
            "depth": 0
        }
    ]
    
    # Test discovered URLs (simulating parser output)
    discovered_urls = [
        {
            "url": "https://example.com/about",
            "source_url": "https://example.com",
            "priority": 50,
            "depth": 1
        },
        {
            "url": "https://example.com/contact",
            "source_url": "https://example.com",
            "priority": 40,
            "depth": 1
        },
        {
            "url": "https://example.com/blog/post-1",
            "source_url": "https://example.com",
            "priority": 60,
            "depth": 1
        },
        # Duplicate URLs for testing deduplication
        {
            "url": "https://example.com/about/",  # Should be deduplicated
            "source_url": "https://example.com",
            "priority": 50,
            "depth": 1
        },
        {
            "url": "https://example.com/blog/post-1?utm_source=twitter",  # Should be deduplicated
            "source_url": "https://example.com",
            "priority": 60,
            "depth": 1
        }
    ]
    
    print("=== Sending Test URLs ===")
    
    # Send seed URLs
    producer.send('seed_urls', value=seed_urls)
    print(f"Sent {len(seed_urls)} seed URLs")
    
    # Wait a bit
    time.sleep(2)
    
    # Send discovered URLs
    producer.send('discovered_urls', value=discovered_urls)
    print(f"Sent {len(discovered_urls)} discovered URLs")
    
    producer.flush()
    producer.close()
    print("All test URLs sent!")

async def test_deduplication():
    """Test deduplication functionality directly"""
    import aioredis
    import asyncpg
    from services.url_frontier.deduplicator import URLDeduplicator
    from services.url_frontier.url_normalizer import URLNormalizer
    
    # Connect to Redis and PostgreSQL
    redis = await aioredis.from_url('redis://localhost:6379')
    db_pool = await asyncpg.create_pool(
        'postgresql://crawler:crawler123@localhost:5432/webcrawler'
    )
    
    normalizer = URLNormalizer()
    deduplicator = URLDeduplicator(redis, db_pool)
    await deduplicator.initialize_bloom_filter()
    
    print("=== Deduplication Test ===")
    
    test_urls = [
        "https://example.com/page1",
        "https://example.com/page1/",  # Should be exact duplicate
        "https://example.com/page1?utm_source=google",  # Should be exact duplicate
        "https://example.com/page-1",  # Should be fuzzy duplicate
        "https://example.com/page2",  # New URL
    ]
    
    for url in test_urls:
        normalized = normalizer.normalize(url)
        if normalized:
            result = await deduplicator.check_duplicate(normalized)
            print(f"URL: {url}")
            print(f"Normalized: {normalized}")
            print(f"Duplicate: {result['is_duplicate']}")
            print(f"Match Type: {result['match_type']}")
            print(f"Similarity: {result['similarity_score']}")
            print("-" * 50)
            
            # Add to cache for next iteration
            if not result['is_duplicate']:
                url_hash = deduplicator._generate_url_hash(normalized)
                await deduplicator.add_url_to_dedup_cache(normalized, url_hash)
    
    # Get stats
    stats = await deduplicator.get_duplicate_stats()
    print("Deduplication Stats:", stats)
    
    await redis.close()
    await db_pool.close()

def main():
    """Main test function"""
    print("Web Crawler URL Frontier Test")
    print("=" * 50)
    
    # Test 1: URL Normalization
    test_url_normalization()
    
    # Test 2: Send test URLs to Kafka
    print("\n" + "=" * 50)
    send_test_urls()
    
    # Test 3: Test deduplication (requires running services)
    print("\n" + "=" * 50)
    print("To test deduplication, run:")
    print("docker-compose up -d redis postgresql")
    print("Then run: python -c 'import asyncio; from test_frontier import test_deduplication; asyncio.run(test_deduplication())'")

if __name__ == "__main__":
    main() 