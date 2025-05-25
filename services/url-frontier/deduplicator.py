"""
URL Deduplication Module

This module implements a two-tier deduplication system:
1. Redis: Fast in-memory lookups using bloom filters and sets
2. PostgreSQL: Persistent storage and fuzzy matching

Fuzzy matching helps identify similar URLs that might be duplicates:
- Levenshtein distance for string similarity
- Content-based similarity (when available)
- Domain-specific rules
"""

import hashlib
import mmh3
from typing import Optional, List, Tuple
import aioredis
import asyncpg
from fuzzywuzzy import fuzz
from bloom_filter2 import BloomFilter
import json
import logging

logger = logging.getLogger(__name__)


class URLDeduplicator:
    def __init__(self, redis_client: aioredis.Redis, db_pool: asyncpg.Pool):
        self.redis = redis_client
        self.db_pool = db_pool
        
        # Fuzzy matching thresholds
        self.fuzzy_threshold = 85  # Minimum similarity score (0-100)
        self.exact_match_threshold = 95  # Consider as exact match
        
        # Redis keys
        self.bloom_filter_key = "url_bloom_filter"
        self.exact_urls_key = "exact_urls"
        self.fuzzy_cache_key = "fuzzy_cache"
        
        # Bloom filter settings (approximate)
        self.bloom_capacity = 10_000_000  # 10M URLs
        self.bloom_error_rate = 0.001  # 0.1% false positive rate

    async def initialize_bloom_filter(self):
        """Initialize or load bloom filter from Redis"""
        try:
            # Check if bloom filter exists in Redis
            exists = await self.redis.exists(self.bloom_filter_key)
            if not exists:
                # Create new bloom filter
                logger.info("Creating new bloom filter")
                await self._create_bloom_filter()
            else:
                logger.info("Bloom filter already exists in Redis")
        except Exception as e:
            logger.error(f"Error initializing bloom filter: {e}")

    async def _create_bloom_filter(self):
        """Create a new bloom filter and populate with existing URLs"""
        try:
            # Get all existing URL hashes from database
            async with self.db_pool.acquire() as conn:
                existing_hashes = await conn.fetch("SELECT url_hash FROM urls")
            
            # Create bloom filter representation in Redis
            # We'll use Redis sets to simulate bloom filter behavior
            if existing_hashes:
                hash_list = [row['url_hash'] for row in existing_hashes]
                await self.redis.sadd(self.bloom_filter_key, *hash_list)
                logger.info(f"Populated bloom filter with {len(hash_list)} existing URLs")
                
        except Exception as e:
            logger.error(f"Error creating bloom filter: {e}")

    def _generate_url_hash(self, normalized_url: str) -> str:
        """Generate hash for exact matching"""
        return hashlib.sha256(normalized_url.encode('utf-8')).hexdigest()

    def _generate_fuzzy_hash(self, normalized_url: str) -> str:
        """Generate hash for fuzzy matching (simplified URL)"""
        # Remove query parameters and fragments for fuzzy comparison
        from urllib.parse import urlparse
        parsed = urlparse(normalized_url)
        simplified = f"{parsed.netloc}{parsed.path}".lower()
        return hashlib.md5(simplified.encode('utf-8')).hexdigest()

    async def is_duplicate_exact(self, normalized_url: str) -> Tuple[bool, Optional[str]]:
        """
        Check for exact duplicates using Redis bloom filter + PostgreSQL
        
        Returns:
            (is_duplicate, existing_url_hash)
        """
        url_hash = self._generate_url_hash(normalized_url)
        
        # Step 1: Check bloom filter (fast, may have false positives)
        bloom_exists = await self.redis.sismember(self.bloom_filter_key, url_hash)
        
        if not bloom_exists:
            # Definitely not a duplicate
            return False, None
            
        # Step 2: Check exact match in Redis cache
        cached_url = await self.redis.hget(self.exact_urls_key, url_hash)
        if cached_url:
            return True, url_hash
            
        # Step 3: Check in PostgreSQL (authoritative)
        async with self.db_pool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT url_hash, url FROM urls WHERE url_hash = $1", 
                url_hash
            )
            
            if result:
                # Cache the result in Redis for future lookups
                await self.redis.hset(self.exact_urls_key, url_hash, result['url'])
                return True, url_hash
                
        return False, None

    async def is_duplicate_fuzzy(self, normalized_url: str) -> Tuple[bool, Optional[str], int]:
        """
        Check for fuzzy duplicates using similarity algorithms
        
        Returns:
            (is_duplicate, similar_url_hash, similarity_score)
        """
        fuzzy_hash = self._generate_fuzzy_hash(normalized_url)
        
        # Check fuzzy cache first
        cached_result = await self.redis.hget(self.fuzzy_cache_key, fuzzy_hash)
        if cached_result:
            cache_data = json.loads(cached_result)
            return cache_data['is_duplicate'], cache_data.get('url_hash'), cache_data['score']
        
        # Extract domain for domain-specific fuzzy matching
        from urllib.parse import urlparse
        domain = urlparse(normalized_url).netloc
        
        # Get similar URLs from the same domain
        async with self.db_pool.acquire() as conn:
            similar_urls = await conn.fetch("""
                SELECT url_hash, normalized_url 
                FROM urls 
                WHERE domain = $1 
                ORDER BY created_at DESC 
                LIMIT 100
            """, domain)
        
        best_match = None
        best_score = 0
        
        for row in similar_urls:
            existing_url = row['normalized_url']
            
            # Calculate similarity using different methods
            ratio_score = fuzz.ratio(normalized_url, existing_url)
            token_score = fuzz.token_sort_ratio(normalized_url, existing_url)
            partial_score = fuzz.partial_ratio(normalized_url, existing_url)
            
            # Use the highest score
            max_score = max(ratio_score, token_score, partial_score)
            
            if max_score > best_score:
                best_score = max_score
                best_match = row['url_hash']
                
        # Determine if it's a duplicate based on threshold
        is_duplicate = best_score >= self.fuzzy_threshold
        
        # Cache the result
        cache_data = {
            'is_duplicate': is_duplicate,
            'url_hash': best_match if is_duplicate else None,
            'score': best_score
        }
        await self.redis.hset(
            self.fuzzy_cache_key, 
            fuzzy_hash, 
            json.dumps(cache_data)
        )
        await self.redis.expire(self.fuzzy_cache_key, 3600)  # 1 hour cache
        
        return is_duplicate, best_match if is_duplicate else None, best_score

    async def check_duplicate(self, normalized_url: str, use_fuzzy: bool = True) -> dict:
        """
        Comprehensive duplicate check
        
        Returns:
            {
                'is_duplicate': bool,
                'match_type': 'exact'|'fuzzy'|'none',
                'existing_url_hash': str|None,
                'similarity_score': int|None
            }
        """
        # First check exact duplicates
        is_exact_dup, exact_hash = await self.is_duplicate_exact(normalized_url)
        
        if is_exact_dup:
            return {
                'is_duplicate': True,
                'match_type': 'exact',
                'existing_url_hash': exact_hash,
                'similarity_score': 100
            }
        
        # Then check fuzzy duplicates if enabled
        if use_fuzzy:
            is_fuzzy_dup, fuzzy_hash, score = await self.is_duplicate_fuzzy(normalized_url)
            
            if is_fuzzy_dup:
                return {
                    'is_duplicate': True,
                    'match_type': 'fuzzy',
                    'existing_url_hash': fuzzy_hash,
                    'similarity_score': score
                }
        
        return {
            'is_duplicate': False,
            'match_type': 'none',
            'existing_url_hash': None,
            'similarity_score': 0
        }

    async def add_url_to_dedup_cache(self, normalized_url: str, url_hash: str):
        """Add URL to deduplication caches after successful insertion"""
        try:
            # Add to bloom filter
            await self.redis.sadd(self.bloom_filter_key, url_hash)
            
            # Add to exact match cache
            await self.redis.hset(self.exact_urls_key, url_hash, normalized_url)
            
            logger.debug(f"Added URL to dedup cache: {normalized_url}")
            
        except Exception as e:
            logger.error(f"Error adding URL to dedup cache: {e}")

    async def get_duplicate_stats(self) -> dict:
        """Get deduplication statistics"""
        try:
            # Count URLs in bloom filter
            bloom_count = await self.redis.scard(self.bloom_filter_key)
            
            # Count cached exact matches
            exact_cache_count = await self.redis.hlen(self.exact_urls_key)
            
            # Count fuzzy cache entries
            fuzzy_cache_count = await self.redis.hlen(self.fuzzy_cache_key)
            
            # Get database stats
            async with self.db_pool.acquire() as conn:
                db_count = await conn.fetchval("SELECT COUNT(*) FROM urls")
                
            return {
                'bloom_filter_size': bloom_count,
                'exact_cache_size': exact_cache_count,
                'fuzzy_cache_size': fuzzy_cache_count,
                'database_urls': db_count
            }
            
        except Exception as e:
            logger.error(f"Error getting duplicate stats: {e}")
            return {}

    async def cleanup_caches(self):
        """Clean up old cache entries"""
        try:
            # Clean fuzzy cache (it has TTL, but we can force cleanup)
            await self.redis.delete(self.fuzzy_cache_key)
            
            # Optionally clean exact cache if it gets too large
            cache_size = await self.redis.hlen(self.exact_urls_key)
            if cache_size > 1_000_000:  # 1M entries
                # Keep only recent entries (this is a simple strategy)
                logger.info("Cleaning exact match cache due to size")
                # In production, you might want a more sophisticated cleanup strategy
                
        except Exception as e:
            logger.error(f"Error cleaning caches: {e}") 