#!/usr/bin/env python3
"""
Content Indexer Service

This service processes parsed content from the Parser service and stores it in Elasticsearch
for full-text search and retrieval. It handles content deduplication, search optimization,
and maintains content freshness.

Architecture Flow:
Parser → Content Indexer → Elasticsearch

Features:
- Elasticsearch document indexing with optimized mappings
- Content deduplication using content hashes
- Search-optimized text processing
- Content freshness tracking and updates
- Bulk indexing for performance
- Index management and optimization
"""

import asyncio
import json
import logging
import os
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any
from urllib.parse import urlparse
import re

import redis.asyncio as redis
from kafka import KafkaProducer, KafkaConsumer
from elasticsearch import Elasticsearch, AsyncElasticsearch
from elasticsearch.helpers import async_bulk, BulkIndexError
import aiofiles
from dateutil import parser as date_parser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ElasticsearchManager:
    """Manage Elasticsearch connections and operations"""
    
    def __init__(self, elasticsearch_url: str):
        self.elasticsearch_url = elasticsearch_url
        self.client = None
        self.sync_client = None
        
        # Index configurations
        self.content_index = "web_content"
        self.links_index = "web_links"
        self.domains_index = "web_domains"
        
    async def initialize(self):
        """Initialize Elasticsearch clients and create indices"""
        try:
            # Initialize async client
            self.client = AsyncElasticsearch([self.elasticsearch_url])
            
            # Initialize sync client for some operations
            self.sync_client = Elasticsearch([self.elasticsearch_url])
            
            # Test connection
            await self.client.ping()
            logger.info("Connected to Elasticsearch")
            
            # Create indices with mappings
            await self.create_indices()
            
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch: {e}")
            raise
    
    async def create_indices(self):
        """Create Elasticsearch indices with optimized mappings"""
        
        # Content index mapping
        content_mapping = {
            "mappings": {
                "properties": {
                    "url": {
                        "type": "keyword",
                        "fields": {
                            "text": {"type": "text", "analyzer": "standard"}
                        }
                    },
                    "final_url": {"type": "keyword"},
                    "domain": {"type": "keyword"},
                    "title": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {
                            "keyword": {"type": "keyword"},
                            "suggest": {"type": "completion"}
                        }
                    },
                    "content": {
                        "type": "text",
                        "analyzer": "standard",
                        "term_vector": "with_positions_offsets"
                    },
                    "content_hash": {"type": "keyword"},
                    "description": {"type": "text", "analyzer": "standard"},
                    "keywords": {"type": "keyword"},
                    "language": {"type": "keyword"},
                    "content_type": {"type": "keyword"},
                    "quality_score": {"type": "float"},
                    "word_count": {"type": "integer"},
                    "reading_time_minutes": {"type": "float"},
                    "flesch_reading_ease": {"type": "float"},
                    "links_count": {"type": "integer"},
                    "internal_links": {"type": "integer"},
                    "external_links": {"type": "integer"},
                    "crawled_at": {"type": "date"},
                    "indexed_at": {"type": "date"},
                    "last_updated": {"type": "date"},
                    "status": {"type": "keyword"},
                    "crawl_depth": {"type": "integer"},
                    "source_url": {"type": "keyword"},
                    "content_length": {"type": "integer"},
                    "response_time": {"type": "float"},
                    "http_status": {"type": "integer"}
                }
            },
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 1,
                "analysis": {
                    "analyzer": {
                        "content_analyzer": {
                            "type": "standard",
                            "stopwords": "_english_"
                        }
                    }
                }
            }
        }
        
        # Links index mapping
        links_mapping = {
            "mappings": {
                "properties": {
                    "source_url": {"type": "keyword"},
                    "target_url": {"type": "keyword"},
                    "anchor_text": {"type": "text", "analyzer": "standard"},
                    "link_type": {"type": "keyword"},
                    "discovered_at": {"type": "date"},
                    "crawl_depth": {"type": "integer"},
                    "domain": {"type": "keyword"},
                    "target_domain": {"type": "keyword"}
                }
            }
        }
        
        # Domains index mapping
        domains_mapping = {
            "mappings": {
                "properties": {
                    "domain": {"type": "keyword"},
                    "pages_count": {"type": "integer"},
                    "avg_quality_score": {"type": "float"},
                    "total_content_length": {"type": "long"},
                    "languages": {"type": "keyword"},
                    "content_types": {"type": "keyword"},
                    "first_crawled": {"type": "date"},
                    "last_crawled": {"type": "date"},
                    "crawl_frequency": {"type": "keyword"},
                    "robots_txt_allowed": {"type": "boolean"},
                    "avg_response_time": {"type": "float"}
                }
            }
        }
        
        # Create indices
        indices = [
            (self.content_index, content_mapping),
            (self.links_index, links_mapping),
            (self.domains_index, domains_mapping)
        ]
        
        for index_name, mapping in indices:
            try:
                if not await self.client.indices.exists(index=index_name):
                    await self.client.indices.create(index=index_name, body=mapping)
                    logger.info(f"Created index: {index_name}")
                else:
                    logger.info(f"Index already exists: {index_name}")
            except Exception as e:
                logger.error(f"Error creating index {index_name}: {e}")
    
    async def close(self):
        """Close Elasticsearch connections"""
        if self.client:
            await self.client.close()
        if self.sync_client:
            self.sync_client.close()


class ContentDeduplicator:
    """Handle content deduplication using various strategies"""
    
    def __init__(self, redis_client, elasticsearch_client):
        self.redis = redis_client
        self.es = elasticsearch_client
        
        # Deduplication settings
        self.content_hash_ttl = 86400 * 30  # 30 days
        self.similarity_threshold = 0.85
        
    async def is_duplicate_content(self, content_hash: str, url: str) -> tuple[bool, Optional[str]]:
        """Check if content is duplicate using hash and similarity"""
        
        # Check exact hash match in Redis cache
        cached_url = await self.redis.get(f"content_hash:{content_hash}")
        if cached_url and cached_url != url:
            return True, cached_url
        
        # Check in Elasticsearch for exact hash
        try:
            response = await self.es.client.search(
                index=self.es.content_index,
                body={
                    "query": {
                        "term": {"content_hash": content_hash}
                    },
                    "_source": ["url", "indexed_at"],
                    "size": 1
                }
            )
            
            if response['hits']['total']['value'] > 0:
                existing_doc = response['hits']['hits'][0]['_source']
                existing_url = existing_doc['url']
                
                if existing_url != url:
                    # Cache the result
                    await self.redis.setex(
                        f"content_hash:{content_hash}",
                        self.content_hash_ttl,
                        existing_url
                    )
                    return True, existing_url
                    
        except Exception as e:
            logger.warning(f"Error checking duplicate content: {e}")
        
        return False, None
    
    async def mark_content_indexed(self, content_hash: str, url: str):
        """Mark content as indexed to prevent future duplicates"""
        await self.redis.setex(
            f"content_hash:{content_hash}",
            self.content_hash_ttl,
            url
        )


class ContentProcessor:
    """Process and optimize content for indexing"""
    
    def __init__(self):
        self.max_content_length = 1000000  # 1MB
        self.max_title_length = 200
        self.max_description_length = 500
        
    def process_content_for_indexing(self, parsed_data: Dict) -> Dict:
        """Process parsed content for optimal Elasticsearch indexing"""
        
        url = parsed_data.get('url', '')
        content_data = parsed_data.get('content', {})
        metadata = parsed_data.get('metadata', {})
        links = parsed_data.get('links', [])
        processing_info = parsed_data.get('processing_info', {})
        fetch_data = parsed_data.get('original_fetch_data', {})
        
        # Extract domain
        domain = urlparse(url).netloc.lower()
        
        # Process content text
        content_text = content_data.get('text', '')
        if len(content_text) > self.max_content_length:
            content_text = content_text[:self.max_content_length] + "..."
        
        # Clean and process title
        title = metadata.get('title', '').strip()
        if len(title) > self.max_title_length:
            title = title[:self.max_title_length] + "..."
        
        # Clean and process description
        description = metadata.get('description', '').strip()
        if len(description) > self.max_description_length:
            description = description[:self.max_description_length] + "..."
        
        # Process keywords
        keywords = metadata.get('keywords', [])
        if isinstance(keywords, str):
            keywords = [k.strip() for k in keywords.split(',')]
        
        # Calculate crawl depth
        crawl_depth = fetch_data.get('depth', 0)
        
        # Prepare document for indexing
        doc = {
            'url': url,
            'final_url': parsed_data.get('final_url', url),
            'domain': domain,
            'title': title,
            'content': content_text,
            'content_hash': content_data.get('content_hash', ''),
            'description': description,
            'keywords': keywords[:20],  # Limit keywords
            'language': metadata.get('detected_language', 'unknown'),
            'content_type': metadata.get('content_type', 'article'),
            'quality_score': metadata.get('quality_score', 0),
            'word_count': metadata.get('word_count', 0),
            'reading_time_minutes': metadata.get('reading_time_minutes', 0),
            'flesch_reading_ease': metadata.get('flesch_reading_ease', 0),
            'links_count': metadata.get('links_count', 0),
            'internal_links': metadata.get('internal_links', 0),
            'external_links': metadata.get('external_links', 0),
            'crawled_at': fetch_data.get('fetched_at'),
            'indexed_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'status': 'indexed',
            'crawl_depth': crawl_depth,
            'content_length': content_data.get('content_length', 0),
            'response_time': fetch_data.get('response_time', 0),
            'http_status': fetch_data.get('status_code', 200)
        }
        
        return doc
    
    def process_links_for_indexing(self, parsed_data: Dict) -> List[Dict]:
        """Process links for indexing"""
        
        source_url = parsed_data.get('url', '')
        source_domain = urlparse(source_url).netloc.lower()
        links = parsed_data.get('links', [])
        crawl_depth = parsed_data.get('original_fetch_data', {}).get('depth', 0)
        
        link_docs = []
        
        for link in links:
            target_url = link.get('url', '')
            target_domain = urlparse(target_url).netloc.lower()
            
            link_doc = {
                'source_url': source_url,
                'target_url': target_url,
                'anchor_text': link.get('anchor_text', ''),
                'link_type': link.get('link_type', 'unknown'),
                'discovered_at': datetime.now().isoformat(),
                'crawl_depth': crawl_depth + 1,
                'domain': source_domain,
                'target_domain': target_domain
            }
            
            link_docs.append(link_doc)
        
        return link_docs


class IndexerService:
    """Main Content Indexer Service"""
    
    def __init__(self):
        # Configuration from environment
        self.kafka_servers = os.getenv('KAFKA_SERVERS', 'localhost:9092').split(',')
        self.redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.elasticsearch_url = os.getenv('ELASTICSEARCH_URL', 'http://localhost:9200')
        self.processing_batch_size = int(os.getenv('PROCESSING_BATCH_SIZE', '10'))
        self.bulk_index_size = int(os.getenv('BULK_INDEX_SIZE', '100'))
        
        # Components
        self.redis = None
        self.producer = None
        self.es_manager = None
        self.deduplicator = None
        self.content_processor = None
        
        # Bulk indexing buffer
        self.content_buffer = []
        self.links_buffer = []
        self.last_bulk_index = time.time()
        self.bulk_index_interval = 30  # seconds
        
        # Statistics
        self.stats = {
            'documents_processed': 0,
            'documents_indexed': 0,
            'documents_deduplicated': 0,
            'links_indexed': 0,
            'indexing_errors': 0,
            'avg_processing_time': 0.0,
            'bulk_operations': 0
        }
        
        logger.info("Content Indexer Service initialized")
    
    async def initialize(self):
        """Initialize all components"""
        try:
            # Initialize Redis
            self.redis = redis.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            logger.info("Connected to Redis")
            
            # Initialize Kafka producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10
            )
            logger.info("Connected to Kafka")
            
            # Initialize Elasticsearch
            self.es_manager = ElasticsearchManager(self.elasticsearch_url)
            await self.es_manager.initialize()
            
            # Initialize processing components
            self.deduplicator = ContentDeduplicator(self.redis, self.es_manager)
            self.content_processor = ContentProcessor()
            
            logger.info("Content Indexer Service fully initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Content Indexer Service: {e}")
            raise
    
    async def process_parsed_content(self, parsed_data: Dict) -> Dict:
        """Process parsed content for indexing"""
        start_time = time.time()
        
        url = parsed_data.get('url', '')
        crawl_id = parsed_data.get('crawl_id', '')
        
        try:
            # Process content for indexing
            content_doc = self.content_processor.process_content_for_indexing(parsed_data)
            content_hash = content_doc.get('content_hash', '')
            
            # Check for duplicate content
            is_duplicate, existing_url = await self.deduplicator.is_duplicate_content(content_hash, url)
            
            if is_duplicate:
                self.stats['documents_deduplicated'] += 1
                logger.info(f"Duplicate content detected: {url} (original: {existing_url})")
                
                return {
                    'success': True,
                    'action': 'deduplicated',
                    'url': url,
                    'crawl_id': crawl_id,
                    'original_url': existing_url,
                    'content_hash': content_hash
                }
            
            # Process links
            link_docs = self.content_processor.process_links_for_indexing(parsed_data)
            
            # Add to bulk indexing buffers
            self.content_buffer.append({
                '_index': self.es_manager.content_index,
                '_id': content_hash,
                '_source': content_doc
            })
            
            for link_doc in link_docs:
                link_id = hashlib.md5(f"{link_doc['source_url']}-{link_doc['target_url']}".encode()).hexdigest()
                self.links_buffer.append({
                    '_index': self.es_manager.links_index,
                    '_id': link_id,
                    '_source': link_doc
                })
            
            # Check if we should perform bulk indexing
            if (len(self.content_buffer) >= self.bulk_index_size or 
                time.time() - self.last_bulk_index > self.bulk_index_interval):
                await self.perform_bulk_indexing()
            
            # Mark content as indexed for deduplication
            await self.deduplicator.mark_content_indexed(content_hash, url)
            
            # Update statistics
            self.stats['documents_processed'] += 1
            
            processing_time = time.time() - start_time
            self.stats['avg_processing_time'] = (
                (self.stats['avg_processing_time'] * (self.stats['documents_processed'] - 1) + processing_time) /
                self.stats['documents_processed']
            )
            
            return {
                'success': True,
                'action': 'indexed',
                'url': url,
                'crawl_id': crawl_id,
                'content_hash': content_hash,
                'quality_score': content_doc.get('quality_score', 0),
                'links_count': len(link_docs),
                'processing_time': processing_time
            }
            
        except Exception as e:
            self.stats['indexing_errors'] += 1
            logger.error(f"Error processing content for {url}: {e}")
            return {
                'success': False,
                'action': 'error',
                'url': url,
                'crawl_id': crawl_id,
                'error': f"Processing error: {str(e)}"
            }
    
    async def perform_bulk_indexing(self):
        """Perform bulk indexing of buffered documents"""
        if not self.content_buffer and not self.links_buffer:
            return
        
        try:
            # Combine all documents for bulk indexing
            all_docs = self.content_buffer + self.links_buffer
            
            if all_docs:
                # Perform bulk indexing
                success_count, failed_items = await async_bulk(
                    self.es_manager.client,
                    all_docs,
                    chunk_size=self.bulk_index_size,
                    request_timeout=60
                )
                
                # Update statistics
                self.stats['documents_indexed'] += len(self.content_buffer)
                self.stats['links_indexed'] += len(self.links_buffer)
                self.stats['bulk_operations'] += 1
                
                if failed_items:
                    self.stats['indexing_errors'] += len(failed_items)
                    logger.warning(f"Bulk indexing failed for {len(failed_items)} documents")
                
                logger.info(f"Bulk indexed {success_count} documents "
                           f"({len(self.content_buffer)} content, {len(self.links_buffer)} links)")
            
            # Clear buffers
            self.content_buffer.clear()
            self.links_buffer.clear()
            self.last_bulk_index = time.time()
            
        except BulkIndexError as e:
            self.stats['indexing_errors'] += len(e.errors)
            logger.error(f"Bulk indexing error: {e}")
        except Exception as e:
            logger.error(f"Unexpected bulk indexing error: {e}")
    
    async def send_indexing_results(self, result: Dict):
        """Send indexing results to appropriate topics"""
        try:
            # Send indexing completion notification
            completion_data = {
                'url': result['url'],
                'crawl_id': result['crawl_id'],
                'success': result['success'],
                'action': result['action'],
                'content_hash': result.get('content_hash'),
                'quality_score': result.get('quality_score', 0),
                'links_count': result.get('links_count', 0),
                'error': result.get('error'),
                'indexed_at': datetime.now().isoformat()
            }
            
            self.producer.send(
                'indexing_completed',
                value=completion_data,
                key=result['crawl_id']
            )
            
        except Exception as e:
            logger.error(f"Error sending indexing results: {e}")
    
    async def process_content_queue(self):
        """Process parsed content from the parser"""
        logger.info("Starting content indexing processor...")
        
        consumer = KafkaConsumer(
            'parsed_content',
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='indexer_service',
            auto_offset_reset='latest',
            max_poll_records=self.processing_batch_size
        )
        
        try:
            for message in consumer:
                parsed_data = message.value
                
                # Process content for indexing
                result = await self.process_parsed_content(parsed_data)
                
                # Send results
                await self.send_indexing_results(result)
                
                # Log progress
                if self.stats['documents_processed'] % 100 == 0:
                    logger.info(f"Processed {self.stats['documents_processed']} documents "
                               f"(Indexed: {self.stats['documents_indexed']}, "
                               f"Deduplicated: {self.stats['documents_deduplicated']}, "
                               f"Errors: {self.stats['indexing_errors']})")
                
        except Exception as e:
            logger.error(f"Error in content indexing processor: {e}")
        finally:
            # Perform final bulk indexing
            await self.perform_bulk_indexing()
            consumer.close()
    
    async def get_indexer_status(self) -> Dict:
        """Get comprehensive indexer status"""
        try:
            # Get Elasticsearch cluster health
            es_health = await self.es_manager.client.cluster.health()
            
            # Get index statistics
            content_stats = await self.es_manager.client.indices.stats(index=self.es_manager.content_index)
            links_stats = await self.es_manager.client.indices.stats(index=self.es_manager.links_index)
            
            return {
                'service': 'indexer',
                'status': 'running',
                'statistics': self.stats.copy(),
                'elasticsearch': {
                    'cluster_status': es_health['status'],
                    'content_documents': content_stats['indices'][self.es_manager.content_index]['total']['docs']['count'],
                    'links_documents': links_stats['indices'][self.es_manager.links_index]['total']['docs']['count'],
                    'content_size': content_stats['indices'][self.es_manager.content_index]['total']['store']['size_in_bytes'],
                    'links_size': links_stats['indices'][self.es_manager.links_index]['total']['store']['size_in_bytes']
                },
                'configuration': {
                    'processing_batch_size': self.processing_batch_size,
                    'bulk_index_size': self.bulk_index_size,
                    'bulk_index_interval': self.bulk_index_interval
                },
                'buffers': {
                    'content_buffer_size': len(self.content_buffer),
                    'links_buffer_size': len(self.links_buffer)
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting indexer status: {e}")
            return {'service': 'indexer', 'status': 'error', 'error': str(e)}
    
    async def run(self):
        """Run the Content Indexer Service"""
        logger.info("Starting Content Indexer Service...")
        
        try:
            await self.process_content_queue()
        except KeyboardInterrupt:
            logger.info("Shutting down Content Indexer Service...")
        finally:
            # Perform final bulk indexing
            await self.perform_bulk_indexing()
            
            if self.es_manager:
                await self.es_manager.close()
            if self.producer:
                self.producer.close()


async def main():
    """Main entry point"""
    indexer = IndexerService()
    
    try:
        await indexer.initialize()
        await indexer.run()
    except KeyboardInterrupt:
        logger.info("Shutting down Content Indexer Service...")
    except Exception as e:
        logger.error(f"Fatal error in Content Indexer Service: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main()) 