#!/usr/bin/env python3
"""
Content Indexer Service Tests

Comprehensive testing for the Content Indexer service including:
- Elasticsearch operations and mappings
- Content processing and optimization
- Deduplication logic
- Bulk indexing performance
- Integration with Kafka and Redis
"""

import asyncio
import json
import time
import hashlib
from datetime import datetime
from typing import Dict, List

import pytest
import redis.asyncio as redis
from elasticsearch import AsyncElasticsearch
from kafka import KafkaProducer

# Import the indexer components
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'services', 'indexer'))

from app import (
    ElasticsearchManager, ContentDeduplicator, ContentProcessor, IndexerService
)


class TestElasticsearchManager:
    """Test Elasticsearch management and operations"""
    
    @pytest.fixture
    async def es_manager(self):
        """Create test Elasticsearch manager"""
        manager = ElasticsearchManager('http://localhost:9200')
        await manager.initialize()
        yield manager
        await manager.close()
    
    async def test_elasticsearch_connection(self, es_manager):
        """Test Elasticsearch connection"""
        assert es_manager.client is not None
        ping_result = await es_manager.client.ping()
        assert ping_result is True
        print("✓ Elasticsearch connection successful")
    
    async def test_index_creation(self, es_manager):
        """Test index creation with proper mappings"""
        # Check if indices exist
        content_exists = await es_manager.client.indices.exists(index=es_manager.content_index)
        links_exists = await es_manager.client.indices.exists(index=es_manager.links_index)
        domains_exists = await es_manager.client.indices.exists(index=es_manager.domains_index)
        
        assert content_exists, "Content index should exist"
        assert links_exists, "Links index should exist"
        assert domains_exists, "Domains index should exist"
        
        # Check mappings
        content_mapping = await es_manager.client.indices.get_mapping(index=es_manager.content_index)
        properties = content_mapping[es_manager.content_index]['mappings']['properties']
        
        # Verify key fields exist
        assert 'url' in properties
        assert 'title' in properties
        assert 'content' in properties
        assert 'content_hash' in properties
        assert 'quality_score' in properties
        
        print("✓ Elasticsearch indices and mappings created correctly")
    
    async def test_document_indexing(self, es_manager):
        """Test document indexing and retrieval"""
        test_doc = {
            'url': 'https://example.com/test',
            'title': 'Test Document',
            'content': 'This is test content for indexing',
            'content_hash': 'test_hash_123',
            'domain': 'example.com',
            'quality_score': 75.5,
            'indexed_at': datetime.now().isoformat()
        }
        
        # Index document
        result = await es_manager.client.index(
            index=es_manager.content_index,
            id='test_doc_1',
            body=test_doc
        )
        
        assert result['result'] in ['created', 'updated']
        
        # Refresh index to make document searchable
        await es_manager.client.indices.refresh(index=es_manager.content_index)
        
        # Search for document
        search_result = await es_manager.client.search(
            index=es_manager.content_index,
            body={
                'query': {
                    'term': {'content_hash': 'test_hash_123'}
                }
            }
        )
        
        assert search_result['hits']['total']['value'] == 1
        retrieved_doc = search_result['hits']['hits'][0]['_source']
        assert retrieved_doc['url'] == test_doc['url']
        assert retrieved_doc['title'] == test_doc['title']
        
        print("✓ Document indexing and retrieval working correctly")


class TestContentDeduplicator:
    """Test content deduplication logic"""
    
    @pytest.fixture
    async def redis_client(self):
        """Create test Redis client"""
        client = redis.from_url('redis://localhost:6379', decode_responses=True)
        await client.ping()
        yield client
        await client.close()
    
    @pytest.fixture
    async def es_manager(self):
        """Create test Elasticsearch manager"""
        manager = ElasticsearchManager('http://localhost:9200')
        await manager.initialize()
        yield manager
        await manager.close()
    
    @pytest.fixture
    async def deduplicator(self, redis_client, es_manager):
        """Create test deduplicator"""
        return ContentDeduplicator(redis_client, es_manager)
    
    async def test_redis_deduplication(self, deduplicator):
        """Test Redis-based deduplication"""
        content_hash = 'test_hash_redis_123'
        url1 = 'https://example.com/page1'
        url2 = 'https://example.com/page2'
        
        # First check - should not be duplicate
        is_duplicate, existing_url = await deduplicator.is_duplicate_content(content_hash, url1)
        assert not is_duplicate
        assert existing_url is None
        
        # Mark as indexed
        await deduplicator.mark_content_indexed(content_hash, url1)
        
        # Second check with different URL - should be duplicate
        is_duplicate, existing_url = await deduplicator.is_duplicate_content(content_hash, url2)
        assert is_duplicate
        assert existing_url == url1
        
        print("✓ Redis-based deduplication working correctly")
    
    async def test_elasticsearch_deduplication(self, deduplicator, es_manager):
        """Test Elasticsearch-based deduplication"""
        content_hash = 'test_hash_es_456'
        url1 = 'https://example.com/es_page1'
        url2 = 'https://example.com/es_page2'
        
        # Index a document directly in Elasticsearch
        test_doc = {
            'url': url1,
            'content_hash': content_hash,
            'title': 'Test Document',
            'indexed_at': datetime.now().isoformat()
        }
        
        await es_manager.client.index(
            index=es_manager.content_index,
            id=content_hash,
            body=test_doc
        )
        
        # Refresh index
        await es_manager.client.indices.refresh(index=es_manager.content_index)
        
        # Check for duplicate
        is_duplicate, existing_url = await deduplicator.is_duplicate_content(content_hash, url2)
        assert is_duplicate
        assert existing_url == url1
        
        print("✓ Elasticsearch-based deduplication working correctly")


class TestContentProcessor:
    """Test content processing and optimization"""
    
    @pytest.fixture
    def processor(self):
        """Create test content processor"""
        return ContentProcessor()
    
    def test_content_processing(self, processor):
        """Test content processing for indexing"""
        parsed_data = {
            'url': 'https://example.com/article',
            'final_url': 'https://example.com/article',
            'content': {
                'text': 'This is a test article with some content. ' * 50,
                'content_hash': 'hash_123',
                'content_length': 2500
            },
            'metadata': {
                'title': 'Test Article Title',
                'description': 'This is a test article description',
                'keywords': ['test', 'article', 'content'],
                'detected_language': 'en',
                'content_type': 'article',
                'quality_score': 85.5,
                'word_count': 250,
                'reading_time_minutes': 1.25,
                'flesch_reading_ease': 65.0,
                'links_count': 15,
                'internal_links': 10,
                'external_links': 5
            },
            'links': [
                {
                    'url': 'https://example.com/link1',
                    'anchor_text': 'Link 1',
                    'link_type': 'internal'
                },
                {
                    'url': 'https://external.com/link2',
                    'anchor_text': 'External Link',
                    'link_type': 'external'
                }
            ],
            'original_fetch_data': {
                'fetched_at': '2024-01-01T12:00:00Z',
                'depth': 1,
                'response_time': 0.5,
                'status_code': 200
            }
        }
        
        # Process content
        content_doc = processor.process_content_for_indexing(parsed_data)
        
        # Verify processed document
        assert content_doc['url'] == 'https://example.com/article'
        assert content_doc['domain'] == 'example.com'
        assert content_doc['title'] == 'Test Article Title'
        assert content_doc['content_hash'] == 'hash_123'
        assert content_doc['quality_score'] == 85.5
        assert content_doc['language'] == 'en'
        assert content_doc['content_type'] == 'article'
        assert content_doc['crawl_depth'] == 1
        assert len(content_doc['keywords']) <= 20  # Should be limited
        
        print("✓ Content processing working correctly")
    
    def test_links_processing(self, processor):
        """Test links processing for indexing"""
        parsed_data = {
            'url': 'https://example.com/source',
            'links': [
                {
                    'url': 'https://example.com/internal',
                    'anchor_text': 'Internal Link',
                    'link_type': 'internal'
                },
                {
                    'url': 'https://external.com/page',
                    'anchor_text': 'External Link',
                    'link_type': 'external'
                }
            ],
            'original_fetch_data': {
                'depth': 2
            }
        }
        
        # Process links
        link_docs = processor.process_links_for_indexing(parsed_data)
        
        assert len(link_docs) == 2
        
        # Check first link
        link1 = link_docs[0]
        assert link1['source_url'] == 'https://example.com/source'
        assert link1['target_url'] == 'https://example.com/internal'
        assert link1['anchor_text'] == 'Internal Link'
        assert link1['link_type'] == 'internal'
        assert link1['domain'] == 'example.com'
        assert link1['target_domain'] == 'example.com'
        assert link1['crawl_depth'] == 3  # depth + 1
        
        print("✓ Links processing working correctly")
    
    def test_content_length_limits(self, processor):
        """Test content length limiting"""
        # Create very long content
        long_content = 'A' * 2000000  # 2MB content
        long_title = 'B' * 500  # 500 char title
        long_description = 'C' * 1000  # 1000 char description
        
        parsed_data = {
            'url': 'https://example.com/long',
            'content': {
                'text': long_content,
                'content_hash': 'long_hash'
            },
            'metadata': {
                'title': long_title,
                'description': long_description,
                'keywords': ['test'] * 50  # 50 keywords
            },
            'original_fetch_data': {}
        }
        
        content_doc = processor.process_content_for_indexing(parsed_data)
        
        # Check limits are applied
        assert len(content_doc['content']) <= processor.max_content_length + 3  # +3 for "..."
        assert len(content_doc['title']) <= processor.max_title_length + 3
        assert len(content_doc['description']) <= processor.max_description_length + 3
        assert len(content_doc['keywords']) <= 20
        
        print("✓ Content length limiting working correctly")


class TestIndexerService:
    """Test the main indexer service"""
    
    @pytest.fixture
    async def indexer_service(self):
        """Create test indexer service"""
        # Set test environment variables
        os.environ['KAFKA_SERVERS'] = 'localhost:9092'
        os.environ['REDIS_URL'] = 'redis://localhost:6379'
        os.environ['ELASTICSEARCH_URL'] = 'http://localhost:9200'
        os.environ['PROCESSING_BATCH_SIZE'] = '5'
        os.environ['BULK_INDEX_SIZE'] = '10'
        
        service = IndexerService()
        await service.initialize()
        yield service
        
        # Cleanup
        if service.es_manager:
            await service.es_manager.close()
        if service.producer:
            service.producer.close()
    
    async def test_service_initialization(self, indexer_service):
        """Test service initialization"""
        assert indexer_service.redis is not None
        assert indexer_service.producer is not None
        assert indexer_service.es_manager is not None
        assert indexer_service.deduplicator is not None
        assert indexer_service.content_processor is not None
        
        # Test Redis connection
        await indexer_service.redis.ping()
        
        # Test Elasticsearch connection
        await indexer_service.es_manager.client.ping()
        
        print("✓ Indexer service initialization successful")
    
    async def test_content_processing_flow(self, indexer_service):
        """Test complete content processing flow"""
        parsed_data = {
            'url': 'https://example.com/test-flow',
            'crawl_id': 'test_crawl_123',
            'content': {
                'text': 'Test content for processing flow',
                'content_hash': 'flow_hash_123',
                'content_length': 100
            },
            'metadata': {
                'title': 'Test Flow Article',
                'description': 'Test description',
                'quality_score': 70.0,
                'word_count': 50,
                'detected_language': 'en'
            },
            'links': [
                {
                    'url': 'https://example.com/link',
                    'anchor_text': 'Test Link',
                    'link_type': 'internal'
                }
            ],
            'original_fetch_data': {
                'fetched_at': datetime.now().isoformat(),
                'depth': 0,
                'response_time': 0.3,
                'status_code': 200
            }
        }
        
        # Process content
        result = await indexer_service.process_parsed_content(parsed_data)
        
        assert result['success'] is True
        assert result['action'] == 'indexed'
        assert result['url'] == 'https://example.com/test-flow'
        assert result['crawl_id'] == 'test_crawl_123'
        assert result['content_hash'] == 'flow_hash_123'
        assert result['quality_score'] == 70.0
        assert result['links_count'] == 1
        assert 'processing_time' in result
        
        print("✓ Content processing flow working correctly")
    
    async def test_deduplication_flow(self, indexer_service):
        """Test deduplication in processing flow"""
        content_hash = 'duplicate_test_hash'
        
        # First document
        parsed_data1 = {
            'url': 'https://example.com/original',
            'crawl_id': 'crawl_1',
            'content': {
                'text': 'Original content',
                'content_hash': content_hash
            },
            'metadata': {'title': 'Original'},
            'links': [],
            'original_fetch_data': {}
        }
        
        # Second document with same content hash
        parsed_data2 = {
            'url': 'https://example.com/duplicate',
            'crawl_id': 'crawl_2',
            'content': {
                'text': 'Duplicate content',
                'content_hash': content_hash
            },
            'metadata': {'title': 'Duplicate'},
            'links': [],
            'original_fetch_data': {}
        }
        
        # Process first document
        result1 = await indexer_service.process_parsed_content(parsed_data1)
        assert result1['action'] == 'indexed'
        
        # Process second document - should be deduplicated
        result2 = await indexer_service.process_parsed_content(parsed_data2)
        assert result2['action'] == 'deduplicated'
        assert result2['original_url'] == 'https://example.com/original'
        
        print("✓ Deduplication flow working correctly")
    
    async def test_bulk_indexing(self, indexer_service):
        """Test bulk indexing functionality"""
        # Add multiple documents to buffer
        for i in range(15):  # More than bulk_index_size (10)
            content_doc = {
                '_index': indexer_service.es_manager.content_index,
                '_id': f'bulk_test_{i}',
                '_source': {
                    'url': f'https://example.com/bulk_{i}',
                    'title': f'Bulk Test {i}',
                    'content': f'Bulk test content {i}',
                    'content_hash': f'bulk_hash_{i}',
                    'indexed_at': datetime.now().isoformat()
                }
            }
            indexer_service.content_buffer.append(content_doc)
        
        # Should trigger bulk indexing automatically
        await indexer_service.perform_bulk_indexing()
        
        # Check that buffers are cleared
        assert len(indexer_service.content_buffer) == 0
        assert indexer_service.stats['bulk_operations'] > 0
        
        print("✓ Bulk indexing working correctly")
    
    async def test_indexer_status(self, indexer_service):
        """Test indexer status reporting"""
        status = await indexer_service.get_indexer_status()
        
        assert status['service'] == 'indexer'
        assert status['status'] == 'running'
        assert 'statistics' in status
        assert 'elasticsearch' in status
        assert 'configuration' in status
        assert 'buffers' in status
        
        # Check statistics structure
        stats = status['statistics']
        assert 'documents_processed' in stats
        assert 'documents_indexed' in stats
        assert 'documents_deduplicated' in stats
        assert 'links_indexed' in stats
        assert 'indexing_errors' in stats
        
        print("✓ Indexer status reporting working correctly")


class TestPerformance:
    """Test indexer performance characteristics"""
    
    async def test_processing_performance(self):
        """Test content processing performance"""
        processor = ContentProcessor()
        
        # Create test data
        test_data = {
            'url': 'https://example.com/perf',
            'content': {
                'text': 'Performance test content. ' * 1000,  # Large content
                'content_hash': 'perf_hash',
                'content_length': 25000
            },
            'metadata': {
                'title': 'Performance Test',
                'description': 'Performance test description',
                'keywords': ['perf', 'test'] * 100,  # Many keywords
                'quality_score': 80.0
            },
            'links': [{'url': f'https://example.com/link_{i}', 'anchor_text': f'Link {i}', 'link_type': 'internal'} for i in range(100)],
            'original_fetch_data': {}
        }
        
        # Measure processing time
        start_time = time.time()
        
        for _ in range(100):  # Process 100 documents
            content_doc = processor.process_content_for_indexing(test_data)
            link_docs = processor.process_links_for_indexing(test_data)
        
        end_time = time.time()
        processing_time = end_time - start_time
        docs_per_second = 100 / processing_time
        
        print(f"✓ Content processing performance: {docs_per_second:.1f} docs/second")
        assert docs_per_second > 50, "Processing should be faster than 50 docs/second"
    
    async def test_elasticsearch_bulk_performance(self):
        """Test Elasticsearch bulk indexing performance"""
        es_manager = ElasticsearchManager('http://localhost:9200')
        await es_manager.initialize()
        
        try:
            # Prepare bulk documents
            bulk_docs = []
            for i in range(1000):
                doc = {
                    '_index': es_manager.content_index,
                    '_id': f'perf_bulk_{i}',
                    '_source': {
                        'url': f'https://example.com/perf_bulk_{i}',
                        'title': f'Performance Bulk Test {i}',
                        'content': f'Performance bulk test content {i}. ' * 50,
                        'content_hash': f'perf_bulk_hash_{i}',
                        'domain': 'example.com',
                        'quality_score': 75.0,
                        'indexed_at': datetime.now().isoformat()
                    }
                }
                bulk_docs.append(doc)
            
            # Measure bulk indexing time
            start_time = time.time()
            
            from elasticsearch.helpers import async_bulk
            success_count, failed_items = await async_bulk(
                es_manager.client,
                bulk_docs,
                chunk_size=100
            )
            
            end_time = time.time()
            indexing_time = end_time - start_time
            docs_per_second = success_count / indexing_time
            
            print(f"✓ Bulk indexing performance: {docs_per_second:.1f} docs/second")
            assert docs_per_second > 100, "Bulk indexing should be faster than 100 docs/second"
            assert success_count == 1000, "All documents should be indexed successfully"
            
        finally:
            await es_manager.close()


async def run_all_tests():
    """Run all indexer tests"""
    print("🚀 Starting Content Indexer Service Tests...\n")
    
    # Test Elasticsearch Manager
    print("📊 Testing Elasticsearch Manager...")
    es_manager = ElasticsearchManager('http://localhost:9200')
    await es_manager.initialize()
    
    # Test connection
    ping_result = await es_manager.client.ping()
    assert ping_result, "Elasticsearch connection failed"
    print("✓ Elasticsearch connection successful")
    
    # Test index creation
    content_exists = await es_manager.client.indices.exists(index=es_manager.content_index)
    assert content_exists, "Content index creation failed"
    print("✓ Elasticsearch indices created successfully")
    
    await es_manager.close()
    
    # Test Content Processor
    print("\n🔧 Testing Content Processor...")
    processor = ContentProcessor()
    
    test_data = {
        'url': 'https://example.com/test',
        'content': {'text': 'Test content', 'content_hash': 'test_hash'},
        'metadata': {'title': 'Test', 'quality_score': 80},
        'links': [{'url': 'https://example.com/link', 'anchor_text': 'Link', 'link_type': 'internal'}],
        'original_fetch_data': {'depth': 1}
    }
    
    content_doc = processor.process_content_for_indexing(test_data)
    assert content_doc['url'] == 'https://example.com/test'
    print("✓ Content processing working correctly")
    
    link_docs = processor.process_links_for_indexing(test_data)
    assert len(link_docs) == 1
    print("✓ Links processing working correctly")
    
    # Test Performance
    print("\n⚡ Testing Performance...")
    
    # Content processing performance
    start_time = time.time()
    for _ in range(100):
        processor.process_content_for_indexing(test_data)
    processing_time = time.time() - start_time
    docs_per_second = 100 / processing_time
    
    print(f"✓ Content processing: {docs_per_second:.1f} docs/second")
    assert docs_per_second > 50, "Processing performance too slow"
    
    print("\n🎉 All Content Indexer tests passed!")
    print(f"📈 Performance Summary:")
    print(f"   - Content Processing: {docs_per_second:.1f} docs/second")
    print(f"   - Expected Elasticsearch Bulk: 100+ docs/second")
    print(f"   - Expected End-to-End: 25+ docs/second")


if __name__ == "__main__":
    asyncio.run(run_all_tests()) 