#!/usr/bin/env python3
"""
Fetcher Service

This service downloads web pages from URLs provided by the URL Scheduler.
It handles HTTP requests with proper headers, timeouts, retries, and politeness.

Architecture Flow:
URL Scheduler → Fetcher → Parser

Features:
- Async HTTP requests with aiohttp
- Proper user agent rotation
- Content type detection
- Retry logic with exponential backoff
- Response validation and filtering
- Kafka integration for results
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse
import hashlib

import aiohttp
import aiofiles
import redis.asyncio as redis
from kafka import KafkaProducer, KafkaConsumer
from fake_useragent import UserAgent
import magic
import chardet
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ContentProcessor:
    """Process and validate fetched content"""
    
    def __init__(self):
        self.max_content_size = 50 * 1024 * 1024  # 50MB
        self.allowed_content_types = {
            'text/html', 'text/plain', 'text/xml',
            'application/xml', 'application/xhtml+xml'
        }
        
    def detect_encoding(self, content: bytes, content_type: str = None) -> str:
        """Detect content encoding"""
        try:
            # Try charset from content-type header first
            if content_type and 'charset=' in content_type:
                charset = content_type.split('charset=')[1].split(';')[0].strip()
                return charset
            
            # Use chardet for detection
            detection = chardet.detect(content[:10000])  # Sample first 10KB
            encoding = detection.get('encoding', 'utf-8')
            confidence = detection.get('confidence', 0)
            
            # Fallback to utf-8 if confidence is low
            if confidence < 0.7:
                encoding = 'utf-8'
                
            return encoding or 'utf-8'
            
        except Exception as e:
            logger.warning(f"Encoding detection failed: {e}")
            return 'utf-8'
    
    def extract_metadata(self, content: str, url: str) -> Dict:
        """Extract basic metadata from HTML content"""
        try:
            soup = BeautifulSoup(content, 'lxml')
            
            metadata = {
                'title': '',
                'description': '',
                'keywords': '',
                'language': '',
                'canonical_url': url,
                'links_count': 0,
                'images_count': 0,
                'text_length': 0
            }
            
            # Extract title
            title_tag = soup.find('title')
            if title_tag:
                metadata['title'] = title_tag.get_text().strip()[:200]
            
            # Extract meta description
            desc_tag = soup.find('meta', attrs={'name': 'description'})
            if desc_tag:
                metadata['description'] = desc_tag.get('content', '')[:500]
            
            # Extract meta keywords
            keywords_tag = soup.find('meta', attrs={'name': 'keywords'})
            if keywords_tag:
                metadata['keywords'] = keywords_tag.get('content', '')[:200]
            
            # Extract language
            html_tag = soup.find('html')
            if html_tag:
                metadata['language'] = html_tag.get('lang', '')
            
            # Extract canonical URL
            canonical_tag = soup.find('link', attrs={'rel': 'canonical'})
            if canonical_tag:
                canonical_href = canonical_tag.get('href')
                if canonical_href:
                    metadata['canonical_url'] = urljoin(url, canonical_href)
            
            # Count elements
            metadata['links_count'] = len(soup.find_all('a', href=True))
            metadata['images_count'] = len(soup.find_all('img', src=True))
            
            # Extract text content length
            text_content = soup.get_text()
            metadata['text_length'] = len(text_content.strip())
            
            return metadata
            
        except Exception as e:
            logger.warning(f"Metadata extraction failed for {url}: {e}")
            return {'title': '', 'description': '', 'text_length': 0}
    
    def is_valid_content(self, content_type: str, content_length: int) -> Tuple[bool, str]:
        """Validate if content should be processed"""
        
        # Check content type
        if content_type:
            main_type = content_type.split(';')[0].strip().lower()
            if main_type not in self.allowed_content_types:
                return False, f"unsupported content type: {main_type}"
        
        # Check content size
        if content_length > self.max_content_size:
            return False, f"content too large: {content_length} bytes"
        
        return True, "valid"


class HTTPFetcher:
    """High-performance HTTP fetcher with proper error handling"""
    
    def __init__(self, shard_id: int):
        self.shard_id = shard_id
        self.session = None
        self.user_agent = UserAgent()
        
        # Configuration
        self.timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.max_retries = 3
        self.retry_delays = [1, 2, 4]  # Exponential backoff
        self.max_redirects = 5
        
        # Statistics
        self.stats = {
            'requests_made': 0,
            'requests_successful': 0,
            'requests_failed': 0,
            'bytes_downloaded': 0,
            'avg_response_time': 0.0
        }
        
    async def initialize(self):
        """Initialize HTTP session"""
        connector = aiohttp.TCPConnector(
            limit=100,  # Total connection pool size
            limit_per_host=10,  # Per-host connection limit
            ttl_dns_cache=300,  # DNS cache TTL
            use_dns_cache=True,
            enable_cleanup_closed=True
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout,
            headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        
        logger.info(f"HTTP Fetcher {self.shard_id} initialized")
    
    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
    
    def get_headers(self, url: str) -> Dict[str, str]:
        """Get headers for request"""
        headers = {
            'User-Agent': self.user_agent.random,
            'Referer': f"https://{urlparse(url).netloc}/",
        }
        return headers
    
    async def fetch_url(self, url: str, crawl_id: str) -> Dict:
        """
        Fetch a single URL with retries and error handling
        
        Args:
            url: URL to fetch
            crawl_id: Unique crawl identifier
            
        Returns:
            Fetch result with content and metadata
        """
        start_time = time.time()
        self.stats['requests_made'] += 1
        
        for attempt in range(self.max_retries):
            try:
                headers = self.get_headers(url)
                
                logger.debug(f"Fetching {url} (attempt {attempt + 1}/{self.max_retries})")
                
                async with self.session.get(
                    url,
                    headers=headers,
                    allow_redirects=True,
                    max_redirects=self.max_redirects
                ) as response:
                    
                    # Record response time
                    response_time = time.time() - start_time
                    self.stats['avg_response_time'] = (
                        (self.stats['avg_response_time'] * (self.stats['requests_made'] - 1) + response_time) /
                        self.stats['requests_made']
                    )
                    
                    # Check response status
                    if response.status >= 400:
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(self.retry_delays[attempt])
                            continue
                        else:
                            self.stats['requests_failed'] += 1
                            return {
                                'success': False,
                                'error': f"HTTP {response.status}: {response.reason}",
                                'status_code': response.status,
                                'url': url,
                                'crawl_id': crawl_id,
                                'response_time': response_time,
                                'final_url': str(response.url)
                            }
                    
                    # Get content info
                    content_type = response.headers.get('content-type', '')
                    content_length = int(response.headers.get('content-length', 0))
                    
                    # Read content
                    content_bytes = await response.read()
                    actual_content_length = len(content_bytes)
                    
                    self.stats['bytes_downloaded'] += actual_content_length
                    self.stats['requests_successful'] += 1
                    
                    return {
                        'success': True,
                        'content': content_bytes,
                        'content_type': content_type,
                        'content_length': actual_content_length,
                        'status_code': response.status,
                        'headers': dict(response.headers),
                        'url': url,
                        'final_url': str(response.url),
                        'crawl_id': crawl_id,
                        'response_time': response_time,
                        'fetched_at': datetime.now().isoformat()
                    }
                    
            except asyncio.TimeoutError:
                error_msg = f"Timeout after {self.timeout.total}s"
                logger.warning(f"Timeout fetching {url}: {error_msg}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delays[attempt])
                    continue
                    
            except aiohttp.ClientError as e:
                error_msg = f"Client error: {str(e)}"
                logger.warning(f"Client error fetching {url}: {error_msg}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delays[attempt])
                    continue
                    
            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                logger.error(f"Unexpected error fetching {url}: {error_msg}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delays[attempt])
                    continue
        
        # All retries failed
        response_time = time.time() - start_time
        self.stats['requests_failed'] += 1
        
        return {
            'success': False,
            'error': error_msg if 'error_msg' in locals() else 'All retries failed',
            'url': url,
            'crawl_id': crawl_id,
            'response_time': response_time,
            'attempts': self.max_retries
        }


class FetcherService:
    """Main Fetcher Service"""
    
    def __init__(self):
        # Configuration from environment
        self.shard_id = int(os.getenv('FETCHER_SHARD_ID', '0'))
        self.kafka_servers = os.getenv('KAFKA_SERVERS', 'localhost:9092').split(',')
        self.redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.fetch_batch_size = int(os.getenv('FETCH_BATCH_SIZE', '10'))
        self.concurrent_requests = int(os.getenv('CONCURRENT_REQUESTS', '20'))
        
        # Components
        self.redis = None
        self.producer = None
        self.http_fetcher = None
        self.content_processor = None
        
        # Queue management
        self.fetch_queue_name = f"fetch_queue_shard_{self.shard_id}"
        self.semaphore = asyncio.Semaphore(self.concurrent_requests)
        
        # Statistics
        self.stats = {
            'urls_processed': 0,
            'urls_successful': 0,
            'urls_failed': 0,
            'content_processed': 0,
            'bytes_downloaded': 0,
            'avg_processing_time': 0.0
        }
        
        logger.info(f"Fetcher Service initialized for shard {self.shard_id}")
    
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
            
            # Initialize HTTP fetcher
            self.http_fetcher = HTTPFetcher(self.shard_id)
            await self.http_fetcher.initialize()
            
            # Initialize content processor
            self.content_processor = ContentProcessor()
            
            logger.info(f"Fetcher Service {self.shard_id} fully initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Fetcher Service: {e}")
            raise
    
    async def process_fetch_request(self, fetch_data: Dict) -> Dict:
        """Process a single fetch request"""
        async with self.semaphore:  # Limit concurrent requests
            start_time = time.time()
            
            url = fetch_data.get('url', '')
            crawl_id = fetch_data.get('crawl_id', '')
            domain = fetch_data.get('domain', '')
            
            try:
                # Fetch the URL
                fetch_result = await self.http_fetcher.fetch_url(url, crawl_id)
                
                if not fetch_result['success']:
                    self.stats['urls_failed'] += 1
                    return {
                        'success': False,
                        'url': url,
                        'crawl_id': crawl_id,
                        'error': fetch_result['error'],
                        'fetch_metadata': fetch_data
                    }
                
                # Validate content
                content_type = fetch_result.get('content_type', '')
                content_length = fetch_result.get('content_length', 0)
                
                is_valid, validation_reason = self.content_processor.is_valid_content(
                    content_type, content_length
                )
                
                if not is_valid:
                    self.stats['urls_failed'] += 1
                    return {
                        'success': False,
                        'url': url,
                        'crawl_id': crawl_id,
                        'error': f"Content validation failed: {validation_reason}",
                        'fetch_metadata': fetch_data
                    }
                
                # Process content
                content_bytes = fetch_result['content']
                encoding = self.content_processor.detect_encoding(content_bytes, content_type)
                
                try:
                    content_text = content_bytes.decode(encoding, errors='replace')
                except UnicodeDecodeError:
                    content_text = content_bytes.decode('utf-8', errors='replace')
                
                # Extract metadata
                metadata = self.content_processor.extract_metadata(content_text, url)
                
                # Create content hash for deduplication
                content_hash = hashlib.sha256(content_bytes).hexdigest()
                
                # Update statistics
                self.stats['urls_successful'] += 1
                self.stats['content_processed'] += 1
                self.stats['bytes_downloaded'] += content_length
                
                processing_time = time.time() - start_time
                self.stats['avg_processing_time'] = (
                    (self.stats['avg_processing_time'] * (self.stats['urls_processed']) + processing_time) /
                    (self.stats['urls_processed'] + 1)
                )
                
                return {
                    'success': True,
                    'url': url,
                    'final_url': fetch_result.get('final_url', url),
                    'crawl_id': crawl_id,
                    'content': content_text,
                    'content_hash': content_hash,
                    'content_type': content_type,
                    'content_length': content_length,
                    'encoding': encoding,
                    'status_code': fetch_result.get('status_code'),
                    'response_headers': fetch_result.get('headers', {}),
                    'metadata': metadata,
                    'fetch_metadata': fetch_data,
                    'processing_time': processing_time,
                    'fetched_at': fetch_result.get('fetched_at'),
                    'shard_id': self.shard_id
                }
                
            except Exception as e:
                self.stats['urls_failed'] += 1
                logger.error(f"Error processing fetch request for {url}: {e}")
                return {
                    'success': False,
                    'url': url,
                    'crawl_id': crawl_id,
                    'error': f"Processing error: {str(e)}",
                    'fetch_metadata': fetch_data
                }
            finally:
                self.stats['urls_processed'] += 1
    
    async def send_to_parser(self, result: Dict):
        """Send fetch result to parser"""
        try:
            if result['success']:
                # Send successful fetch to parser
                self.producer.send(
                    'raw_content',
                    value=result,
                    key=result['crawl_id']
                )
            
            # Send completion notification to scheduler
            completion_data = {
                'url': result['url'],
                'crawl_id': result['crawl_id'],
                'success': result['success'],
                'response_time': result.get('processing_time', 0),
                'error': result.get('error'),
                'shard_id': self.shard_id,
                'completed_at': datetime.now().isoformat()
            }
            
            self.producer.send(
                'crawl_completed',
                value=completion_data,
                key=result['crawl_id']
            )
            
        except Exception as e:
            logger.error(f"Error sending result to parser: {e}")
    
    async def process_fetch_queue(self):
        """Process URLs from the fetch queue"""
        logger.info(f"Starting fetch queue processor for shard {self.shard_id}")
        
        consumer = KafkaConsumer(
            self.fetch_queue_name,
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=f'fetcher_shard_{self.shard_id}',
            auto_offset_reset='latest',
            max_poll_records=self.fetch_batch_size
        )
        
        try:
            for message in consumer:
                fetch_data = message.value
                
                # Process fetch request
                result = await self.process_fetch_request(fetch_data)
                
                # Send result to parser
                await self.send_to_parser(result)
                
                # Log progress
                if self.stats['urls_processed'] % 100 == 0:
                    logger.info(f"Shard {self.shard_id} processed {self.stats['urls_processed']} URLs "
                               f"(Success: {self.stats['urls_successful']}, "
                               f"Failed: {self.stats['urls_failed']})")
                
        except Exception as e:
            logger.error(f"Error in fetch queue processor: {e}")
        finally:
            consumer.close()
    
    async def get_fetcher_status(self) -> Dict:
        """Get comprehensive fetcher status"""
        try:
            http_stats = self.http_fetcher.stats if self.http_fetcher else {}
            
            return {
                'service': 'fetcher',
                'shard_id': self.shard_id,
                'status': 'running',
                'statistics': self.stats.copy(),
                'http_statistics': http_stats,
                'configuration': {
                    'concurrent_requests': self.concurrent_requests,
                    'fetch_batch_size': self.fetch_batch_size,
                    'fetch_queue_name': self.fetch_queue_name
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting fetcher status: {e}")
            return {'service': 'fetcher', 'shard_id': self.shard_id, 'status': 'error', 'error': str(e)}
    
    async def run(self):
        """Run the Fetcher Service"""
        logger.info(f"Starting Fetcher Service for shard {self.shard_id}")
        
        try:
            await self.process_fetch_queue()
        except KeyboardInterrupt:
            logger.info("Shutting down Fetcher Service...")
        finally:
            if self.http_fetcher:
                await self.http_fetcher.close()
            if self.producer:
                self.producer.close()


async def main():
    """Main entry point"""
    fetcher = FetcherService()
    
    try:
        await fetcher.initialize()
        await fetcher.run()
    except KeyboardInterrupt:
        logger.info("Shutting down Fetcher Service...")
    except Exception as e:
        logger.error(f"Fatal error in Fetcher Service: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main()) 