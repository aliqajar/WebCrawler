#!/usr/bin/env python3
"""
Parser Service

This service processes raw HTML content from the Fetcher service to extract:
1. Links for further crawling
2. Clean text content for indexing
3. Structured metadata
4. Content quality metrics

Architecture Flow:
Fetcher → Parser → Content Indexer + URL Frontier

Features:
- Advanced link extraction and normalization
- Content cleaning and text extraction
- Language detection and readability analysis
- Duplicate content detection
- Content quality scoring
"""

import asyncio
import json
import logging
import os
import re
import time
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse
from collections import defaultdict

import redis.asyncio as redis
from kafka import KafkaProducer, KafkaConsumer
from bs4 import BeautifulSoup, Comment
import trafilatura
import justext
from boilerpy3 import extractors
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
import textstat
from langdetect import detect, LangDetectError
import tldextract
import validators

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LinkExtractor:
    """Extract and normalize links from HTML content"""
    
    def __init__(self):
        # URL patterns to exclude
        self.excluded_extensions = {
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.zip', '.rar', '.tar', '.gz', '.7z',
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp',
            '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv',
            '.css', '.js', '.ico', '.xml', '.rss'
        }
        
        # URL schemes to include
        self.allowed_schemes = {'http', 'https'}
        
        # Common social media and external domains to exclude
        self.excluded_domains = {
            'facebook.com', 'twitter.com', 'instagram.com', 'linkedin.com',
            'youtube.com', 'tiktok.com', 'pinterest.com', 'reddit.com',
            'google.com', 'bing.com', 'yahoo.com', 'duckduckgo.com'
        }
    
    def normalize_url(self, url: str, base_url: str) -> Optional[str]:
        """Normalize and validate a URL"""
        try:
            # Convert relative URLs to absolute
            absolute_url = urljoin(base_url, url.strip())
            
            # Parse URL
            parsed = urlparse(absolute_url)
            
            # Check scheme
            if parsed.scheme not in self.allowed_schemes:
                return None
            
            # Check for excluded file extensions
            path_lower = parsed.path.lower()
            if any(path_lower.endswith(ext) for ext in self.excluded_extensions):
                return None
            
            # Check for excluded domains
            domain = parsed.netloc.lower()
            if any(excluded in domain for excluded in self.excluded_domains):
                return None
            
            # Remove fragment and normalize
            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc.lower(),
                parsed.path.rstrip('/') if parsed.path != '/' else '/',
                parsed.params,
                parsed.query,
                ''  # Remove fragment
            ))
            
            # Validate final URL
            if validators.url(normalized):
                return normalized
            
            return None
            
        except Exception as e:
            logger.debug(f"URL normalization failed for {url}: {e}")
            return None
    
    def extract_links(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Extract all valid links from HTML"""
        links = []
        seen_urls = set()
        
        # Extract from <a> tags
        for link_tag in soup.find_all('a', href=True):
            href = link_tag.get('href')
            if not href:
                continue
            
            normalized_url = self.normalize_url(href, base_url)
            if normalized_url and normalized_url not in seen_urls:
                seen_urls.add(normalized_url)
                
                # Extract link metadata
                link_data = {
                    'url': normalized_url,
                    'anchor_text': link_tag.get_text().strip()[:200],
                    'title': link_tag.get('title', ''),
                    'rel': link_tag.get('rel', []),
                    'link_type': 'internal' if self._is_internal_link(normalized_url, base_url) else 'external'
                }
                links.append(link_data)
        
        # Extract from <link> tags (canonical, alternate, etc.)
        for link_tag in soup.find_all('link', href=True):
            href = link_tag.get('href')
            rel = link_tag.get('rel', [])
            
            if 'canonical' in rel or 'alternate' in rel:
                normalized_url = self.normalize_url(href, base_url)
                if normalized_url and normalized_url not in seen_urls:
                    seen_urls.add(normalized_url)
                    
                    link_data = {
                        'url': normalized_url,
                        'anchor_text': '',
                        'title': '',
                        'rel': rel,
                        'link_type': 'canonical' if 'canonical' in rel else 'alternate'
                    }
                    links.append(link_data)
        
        return links
    
    def _is_internal_link(self, url: str, base_url: str) -> bool:
        """Check if a link is internal to the same domain"""
        try:
            url_domain = tldextract.extract(url).registered_domain
            base_domain = tldextract.extract(base_url).registered_domain
            return url_domain == base_domain
        except:
            return False


class ContentExtractor:
    """Extract and clean text content from HTML"""
    
    def __init__(self):
        # Initialize extractors
        self.trafilatura_config = trafilatura.settings.use_config()
        self.trafilatura_config.set("DEFAULT", "EXTRACTION_TIMEOUT", "30")
        
        # Initialize NLTK components
        try:
            self.stop_words = set(stopwords.words('english'))
        except LookupError:
            self.stop_words = set()
    
    def extract_content(self, html: str, url: str) -> Dict:
        """Extract clean content using multiple methods"""
        content_results = {}
        
        # Method 1: Trafilatura (best for news articles)
        try:
            trafilatura_content = trafilatura.extract(
                html, 
                config=self.trafilatura_config,
                include_comments=False,
                include_tables=True
            )
            content_results['trafilatura'] = trafilatura_content or ""
        except Exception as e:
            logger.debug(f"Trafilatura extraction failed for {url}: {e}")
            content_results['trafilatura'] = ""
        
        # Method 2: JusText (good for general content)
        try:
            paragraphs = justext.justext(html, justext.get_stoplist("English"))
            justext_content = "\n".join([p.text for p in paragraphs if not p.is_boilerplate])
            content_results['justext'] = justext_content
        except Exception as e:
            logger.debug(f"JusText extraction failed for {url}: {e}")
            content_results['justext'] = ""
        
        # Method 3: BoilerPy3 (alternative approach)
        try:
            extractor = extractors.ArticleExtractor()
            boilerpy_content = extractor.get_content(html)
            content_results['boilerpy'] = boilerpy_content or ""
        except Exception as e:
            logger.debug(f"BoilerPy extraction failed for {url}: {e}")
            content_results['boilerpy'] = ""
        
        # Method 4: BeautifulSoup fallback
        try:
            soup = BeautifulSoup(html, 'lxml')
            # Remove script and style elements
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            
            # Remove comments
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                comment.extract()
            
            # Get text content
            soup_content = soup.get_text()
            # Clean up whitespace
            lines = (line.strip() for line in soup_content.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            soup_content = ' '.join(chunk for chunk in chunks if chunk)
            content_results['beautifulsoup'] = soup_content
        except Exception as e:
            logger.debug(f"BeautifulSoup extraction failed for {url}: {e}")
            content_results['beautifulsoup'] = ""
        
        # Choose best content
        best_content = self._select_best_content(content_results)
        
        return {
            'content': best_content,
            'extraction_methods': content_results,
            'content_length': len(best_content),
            'word_count': len(best_content.split()) if best_content else 0
        }
    
    def _select_best_content(self, content_results: Dict[str, str]) -> str:
        """Select the best extracted content based on length and quality"""
        # Filter out empty results
        valid_results = {k: v for k, v in content_results.items() if v and len(v.strip()) > 100}
        
        if not valid_results:
            return ""
        
        # Prefer trafilatura for its quality, then justext, then others
        preference_order = ['trafilatura', 'justext', 'boilerpy', 'beautifulsoup']
        
        for method in preference_order:
            if method in valid_results:
                content = valid_results[method].strip()
                if len(content) > 200:  # Minimum content length
                    return content
        
        # Fallback to longest content
        return max(valid_results.values(), key=len)


class ContentAnalyzer:
    """Analyze content quality and characteristics"""
    
    def __init__(self):
        try:
            self.stop_words = set(stopwords.words('english'))
        except LookupError:
            self.stop_words = set()
    
    def analyze_content(self, content: str, metadata: Dict) -> Dict:
        """Perform comprehensive content analysis"""
        if not content or len(content.strip()) < 50:
            return self._empty_analysis()
        
        analysis = {}
        
        # Basic metrics
        analysis['character_count'] = len(content)
        analysis['word_count'] = len(content.split())
        analysis['sentence_count'] = len(sent_tokenize(content))
        analysis['paragraph_count'] = len([p for p in content.split('\n\n') if p.strip()])
        
        # Readability metrics
        try:
            analysis['flesch_reading_ease'] = textstat.flesch_reading_ease(content)
            analysis['flesch_kincaid_grade'] = textstat.flesch_kincaid_grade(content)
            analysis['automated_readability_index'] = textstat.automated_readability_index(content)
            analysis['reading_time_minutes'] = analysis['word_count'] / 200  # Average reading speed
        except:
            analysis.update({
                'flesch_reading_ease': 0,
                'flesch_kincaid_grade': 0,
                'automated_readability_index': 0,
                'reading_time_minutes': 0
            })
        
        # Language detection
        try:
            analysis['detected_language'] = detect(content)
            analysis['language_confidence'] = 0.8  # Placeholder
        except LangDetectError:
            analysis['detected_language'] = 'unknown'
            analysis['language_confidence'] = 0.0
        
        # Content quality score (0-100)
        analysis['quality_score'] = self._calculate_quality_score(content, analysis, metadata)
        
        # Content type classification
        analysis['content_type'] = self._classify_content_type(content, metadata)
        
        # Keyword extraction (simple approach)
        analysis['keywords'] = self._extract_keywords(content)
        
        return analysis
    
    def _empty_analysis(self) -> Dict:
        """Return empty analysis for invalid content"""
        return {
            'character_count': 0,
            'word_count': 0,
            'sentence_count': 0,
            'paragraph_count': 0,
            'flesch_reading_ease': 0,
            'flesch_kincaid_grade': 0,
            'automated_readability_index': 0,
            'reading_time_minutes': 0,
            'detected_language': 'unknown',
            'language_confidence': 0.0,
            'quality_score': 0,
            'content_type': 'unknown',
            'keywords': []
        }
    
    def _calculate_quality_score(self, content: str, analysis: Dict, metadata: Dict) -> float:
        """Calculate content quality score (0-100)"""
        score = 0.0
        
        # Length score (0-25 points)
        word_count = analysis['word_count']
        if word_count >= 300:
            score += 25
        elif word_count >= 150:
            score += 15
        elif word_count >= 50:
            score += 10
        
        # Structure score (0-20 points)
        if analysis['paragraph_count'] >= 3:
            score += 10
        if analysis['sentence_count'] >= 5:
            score += 10
        
        # Readability score (0-20 points)
        flesch_score = analysis.get('flesch_reading_ease', 0)
        if 60 <= flesch_score <= 80:  # Good readability
            score += 20
        elif 40 <= flesch_score <= 90:  # Acceptable readability
            score += 15
        elif flesch_score > 0:
            score += 10
        
        # Metadata quality (0-15 points)
        title = metadata.get('title', '')
        description = metadata.get('description', '')
        if title and len(title) > 10:
            score += 8
        if description and len(description) > 20:
            score += 7
        
        # Content uniqueness (0-20 points)
        # Simple heuristic: check for repetitive patterns
        unique_words = len(set(content.lower().split()))
        total_words = len(content.split())
        if total_words > 0:
            uniqueness_ratio = unique_words / total_words
            score += min(20, uniqueness_ratio * 25)
        
        return min(100.0, score)
    
    def _classify_content_type(self, content: str, metadata: Dict) -> str:
        """Classify content type based on patterns"""
        content_lower = content.lower()
        title = metadata.get('title', '').lower()
        
        # News article indicators
        news_indicators = ['published', 'reporter', 'breaking', 'news', 'update']
        if any(indicator in content_lower or indicator in title for indicator in news_indicators):
            return 'news'
        
        # Blog post indicators
        blog_indicators = ['posted by', 'author:', 'blog', 'opinion', 'thoughts']
        if any(indicator in content_lower or indicator in title for indicator in blog_indicators):
            return 'blog'
        
        # Product page indicators
        product_indicators = ['price', 'buy now', 'add to cart', 'product', 'review']
        if any(indicator in content_lower for indicator in product_indicators):
            return 'product'
        
        # Documentation indicators
        doc_indicators = ['documentation', 'api', 'tutorial', 'guide', 'how to']
        if any(indicator in content_lower or indicator in title for indicator in doc_indicators):
            return 'documentation'
        
        return 'article'  # Default
    
    def _extract_keywords(self, content: str, max_keywords: int = 10) -> List[str]:
        """Extract keywords using simple frequency analysis"""
        try:
            # Tokenize and filter
            words = word_tokenize(content.lower())
            words = [word for word in words if word.isalpha() and len(word) > 3]
            words = [word for word in words if word not in self.stop_words]
            
            # Count frequency
            word_freq = defaultdict(int)
            for word in words:
                word_freq[word] += 1
            
            # Return top keywords
            return [word for word, freq in sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:max_keywords]]
        except:
            return []


class ParserService:
    """Main Parser Service"""
    
    def __init__(self):
        # Configuration from environment
        self.kafka_servers = os.getenv('KAFKA_SERVERS', 'localhost:9092').split(',')
        self.redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.processing_batch_size = int(os.getenv('PROCESSING_BATCH_SIZE', '10'))
        self.min_content_length = int(os.getenv('MIN_CONTENT_LENGTH', '100'))
        
        # Components
        self.redis = None
        self.producer = None
        self.link_extractor = None
        self.content_extractor = None
        self.content_analyzer = None
        
        # Statistics
        self.stats = {
            'pages_processed': 0,
            'pages_successful': 0,
            'pages_failed': 0,
            'links_extracted': 0,
            'content_extracted': 0,
            'avg_processing_time': 0.0
        }
        
        logger.info("Parser Service initialized")
    
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
            
            # Initialize processing components
            self.link_extractor = LinkExtractor()
            self.content_extractor = ContentExtractor()
            self.content_analyzer = ContentAnalyzer()
            
            logger.info("Parser Service fully initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Parser Service: {e}")
            raise
    
    async def process_content(self, raw_content_data: Dict) -> Dict:
        """Process raw content from fetcher"""
        start_time = time.time()
        
        url = raw_content_data.get('url', '')
        crawl_id = raw_content_data.get('crawl_id', '')
        html_content = raw_content_data.get('content', '')
        
        try:
            # Validate input
            if not html_content or len(html_content) < self.min_content_length:
                self.stats['pages_failed'] += 1
                return {
                    'success': False,
                    'url': url,
                    'crawl_id': crawl_id,
                    'error': 'Content too short or empty'
                }
            
            # Parse HTML
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Extract links
            links = self.link_extractor.extract_links(soup, url)
            
            # Extract content
            content_result = self.content_extractor.extract_content(html_content, url)
            clean_content = content_result['content']
            
            if not clean_content or len(clean_content) < self.min_content_length:
                self.stats['pages_failed'] += 1
                return {
                    'success': False,
                    'url': url,
                    'crawl_id': crawl_id,
                    'error': 'No meaningful content extracted'
                }
            
            # Get existing metadata from fetcher
            existing_metadata = raw_content_data.get('metadata', {})
            
            # Analyze content
            content_analysis = self.content_analyzer.analyze_content(clean_content, existing_metadata)
            
            # Create content hash for deduplication
            content_hash = hashlib.sha256(clean_content.encode('utf-8')).hexdigest()
            
            # Update statistics
            self.stats['pages_successful'] += 1
            self.stats['links_extracted'] += len(links)
            self.stats['content_extracted'] += 1
            
            processing_time = time.time() - start_time
            self.stats['avg_processing_time'] = (
                (self.stats['avg_processing_time'] * self.stats['pages_processed'] + processing_time) /
                (self.stats['pages_processed'] + 1)
            )
            
            # Prepare result
            result = {
                'success': True,
                'url': url,
                'final_url': raw_content_data.get('final_url', url),
                'crawl_id': crawl_id,
                'content': {
                    'text': clean_content,
                    'html': html_content,
                    'content_hash': content_hash,
                    'extraction_method': 'multi-method',
                    'content_length': len(clean_content),
                    'word_count': content_result['word_count']
                },
                'links': links,
                'metadata': {
                    **existing_metadata,  # Include fetcher metadata
                    **content_analysis,   # Add content analysis
                    'links_count': len(links),
                    'internal_links': len([l for l in links if l['link_type'] == 'internal']),
                    'external_links': len([l for l in links if l['link_type'] == 'external'])
                },
                'processing_info': {
                    'processed_at': datetime.now().isoformat(),
                    'processing_time': processing_time,
                    'parser_version': '1.0'
                },
                'original_fetch_data': {
                    'fetched_at': raw_content_data.get('fetched_at'),
                    'status_code': raw_content_data.get('status_code'),
                    'content_type': raw_content_data.get('content_type'),
                    'shard_id': raw_content_data.get('shard_id')
                }
            }
            
            return result
            
        except Exception as e:
            self.stats['pages_failed'] += 1
            logger.error(f"Error processing content for {url}: {e}")
            return {
                'success': False,
                'url': url,
                'crawl_id': crawl_id,
                'error': f"Processing error: {str(e)}"
            }
        finally:
            self.stats['pages_processed'] += 1
    
    async def send_results(self, result: Dict):
        """Send parsing results to appropriate topics"""
        try:
            if result['success']:
                # Send processed content to indexer
                self.producer.send(
                    'parsed_content',
                    value=result,
                    key=result['crawl_id']
                )
                
                # Send discovered links back to frontier
                links = result.get('links', [])
                if links:
                    # Filter and prepare links for frontier
                    new_urls = []
                    for link in links:
                        if link['link_type'] in ['internal', 'external']:  # Skip canonical/alternate
                            url_data = {
                                'url': link['url'],
                                'source_url': result['url'],
                                'anchor_text': link['anchor_text'],
                                'discovered_at': datetime.now().isoformat(),
                                'depth': result.get('original_fetch_data', {}).get('depth', 0) + 1,
                                'priority': 5  # Default priority
                            }
                            new_urls.append(url_data)
                    
                    if new_urls:
                        # Send to discovered URLs topic
                        for url_data in new_urls:
                            self.producer.send(
                                'discovered_urls',
                                value=url_data,
                                key=url_data['url']
                            )
            
            # Send processing completion notification
            completion_data = {
                'url': result['url'],
                'crawl_id': result['crawl_id'],
                'success': result['success'],
                'links_found': len(result.get('links', [])),
                'content_quality': result.get('metadata', {}).get('quality_score', 0),
                'error': result.get('error'),
                'processed_at': datetime.now().isoformat()
            }
            
            self.producer.send(
                'parsing_completed',
                value=completion_data,
                key=result['crawl_id']
            )
            
        except Exception as e:
            logger.error(f"Error sending results: {e}")
    
    async def process_content_queue(self):
        """Process raw content from the fetcher"""
        logger.info("Starting content queue processor...")
        
        consumer = KafkaConsumer(
            'raw_content',
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='parser_service',
            auto_offset_reset='latest',
            max_poll_records=self.processing_batch_size
        )
        
        try:
            for message in consumer:
                raw_content_data = message.value
                
                # Process content
                result = await self.process_content(raw_content_data)
                
                # Send results
                await self.send_results(result)
                
                # Log progress
                if self.stats['pages_processed'] % 100 == 0:
                    logger.info(f"Processed {self.stats['pages_processed']} pages "
                               f"(Success: {self.stats['pages_successful']}, "
                               f"Failed: {self.stats['pages_failed']}, "
                               f"Links: {self.stats['links_extracted']})")
                
        except Exception as e:
            logger.error(f"Error in content queue processor: {e}")
        finally:
            consumer.close()
    
    async def get_parser_status(self) -> Dict:
        """Get comprehensive parser status"""
        try:
            return {
                'service': 'parser',
                'status': 'running',
                'statistics': self.stats.copy(),
                'configuration': {
                    'processing_batch_size': self.processing_batch_size,
                    'min_content_length': self.min_content_length
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting parser status: {e}")
            return {'service': 'parser', 'status': 'error', 'error': str(e)}
    
    async def run(self):
        """Run the Parser Service"""
        logger.info("Starting Parser Service...")
        
        try:
            await self.process_content_queue()
        except KeyboardInterrupt:
            logger.info("Shutting down Parser Service...")
        finally:
            if self.producer:
                self.producer.close()


async def main():
    """Main entry point"""
    parser = ParserService()
    
    try:
        await parser.initialize()
        await parser.run()
    except KeyboardInterrupt:
        logger.info("Shutting down Parser Service...")
    except Exception as e:
        logger.error(f"Fatal error in Parser Service: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main()) 