"""
Politeness Manager Module

This module handles web crawling politeness rules:
1. Robots.txt compliance
2. Crawl delays between requests to same domain
3. Concurrent request limits per domain
4. Domain-specific crawling rules
5. Rate limiting and backoff strategies
"""

import asyncio
import aiohttp
import aioredis
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
import logging
import json

logger = logging.getLogger(__name__)


class PolitenessManager:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client
        
        # Default politeness settings
        self.default_crawl_delay = 1.0  # seconds
        self.max_concurrent_per_domain = 2
        self.robots_cache_ttl = 3600  # 1 hour
        self.user_agent = "WebCrawler/1.0 (+http://example.com/bot)"
        
        # Redis keys
        self.robots_cache_key = "robots_cache"
        self.domain_delays_key = "domain_delays"
        self.active_crawls_key = "active_crawls"
        self.domain_stats_key = "domain_stats"
        
        # Rate limiting
        self.rate_limit_window = 60  # 1 minute
        self.max_requests_per_minute = 60
        
    async def can_crawl_url(self, url: str) -> Tuple[bool, str, float]:
        """
        Check if URL can be crawled based on politeness rules
        
        Returns:
            (can_crawl, reason, delay_seconds)
        """
        domain = self._extract_domain(url)
        
        # Check robots.txt
        robots_allowed, robots_reason = await self._check_robots_txt(url)
        if not robots_allowed:
            return False, robots_reason, 0
        
        # Check crawl delay
        can_crawl_now, delay_reason, delay = await self._check_crawl_delay(domain)
        if not can_crawl_now:
            return False, delay_reason, delay
        
        # Check concurrent requests
        can_crawl_concurrent, concurrent_reason = await self._check_concurrent_limit(domain)
        if not can_crawl_concurrent:
            return False, concurrent_reason, self.default_crawl_delay
        
        # Check rate limiting
        can_crawl_rate, rate_reason = await self._check_rate_limit(domain)
        if not can_crawl_rate:
            return False, rate_reason, 10.0  # Longer delay for rate limiting
        
        return True, "allowed", 0

    async def _check_robots_txt(self, url: str) -> Tuple[bool, str]:
        """Check robots.txt compliance"""
        try:
            domain = self._extract_domain(url)
            robots_url = f"https://{domain}/robots.txt"
            
            # Check cache first
            cached_robots = await self.redis.hget(self.robots_cache_key, domain)
            
            if cached_robots:
                robots_data = json.loads(cached_robots)
                rp = RobotFileParser()
                rp.set_url(robots_url)
                
                # Reconstruct robots.txt content
                if robots_data.get('content'):
                    # Simple check - in production, you'd want more sophisticated parsing
                    content = robots_data['content'].lower()
                    if 'disallow: /' in content and self.user_agent.lower() in content:
                        return False, "robots.txt disallows crawling"
                
                return True, "robots.txt allows crawling"
            
            # Fetch robots.txt
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            content = await response.text()
                            
                            # Cache robots.txt
                            robots_data = {
                                'content': content,
                                'fetched_at': datetime.now().isoformat(),
                                'status': response.status
                            }
                            await self.redis.hset(
                                self.robots_cache_key, 
                                domain, 
                                json.dumps(robots_data)
                            )
                            await self.redis.expire(self.robots_cache_key, self.robots_cache_ttl)
                            
                            # Parse robots.txt
                            rp = RobotFileParser()
                            rp.set_url(robots_url)
                            rp.read()
                            
                            if not rp.can_fetch(self.user_agent, url):
                                return False, "robots.txt disallows crawling"
                        
                        else:
                            # No robots.txt or error - assume allowed
                            robots_data = {
                                'content': None,
                                'fetched_at': datetime.now().isoformat(),
                                'status': response.status
                            }
                            await self.redis.hset(
                                self.robots_cache_key, 
                                domain, 
                                json.dumps(robots_data)
                            )
                            
            except Exception as e:
                logger.warning(f"Failed to fetch robots.txt for {domain}: {e}")
                # Assume allowed if we can't fetch robots.txt
                pass
            
            return True, "robots.txt allows crawling"
            
        except Exception as e:
            logger.error(f"Error checking robots.txt for {url}: {e}")
            return True, "robots.txt check failed, assuming allowed"

    async def _check_crawl_delay(self, domain: str) -> Tuple[bool, str, float]:
        """Check if enough time has passed since last crawl to this domain"""
        try:
            last_crawl_key = f"{self.domain_delays_key}:{domain}"
            last_crawl_str = await self.redis.get(last_crawl_key)
            
            if last_crawl_str:
                last_crawl = datetime.fromisoformat(last_crawl_str.decode())
                time_since_last = (datetime.now() - last_crawl).total_seconds()
                
                # Get domain-specific crawl delay
                crawl_delay = await self._get_domain_crawl_delay(domain)
                
                if time_since_last < crawl_delay:
                    remaining_delay = crawl_delay - time_since_last
                    return False, f"crawl delay not met (need {remaining_delay:.1f}s)", remaining_delay
            
            return True, "crawl delay satisfied", 0
            
        except Exception as e:
            logger.error(f"Error checking crawl delay for {domain}: {e}")
            return True, "crawl delay check failed, assuming allowed", 0

    async def _check_concurrent_limit(self, domain: str) -> Tuple[bool, str]:
        """Check if domain has reached concurrent crawl limit"""
        try:
            active_key = f"{self.active_crawls_key}:{domain}"
            active_count = await self.redis.scard(active_key)
            
            if active_count >= self.max_concurrent_per_domain:
                return False, f"concurrent limit reached ({active_count}/{self.max_concurrent_per_domain})"
            
            return True, "concurrent limit ok"
            
        except Exception as e:
            logger.error(f"Error checking concurrent limit for {domain}: {e}")
            return True, "concurrent limit check failed, assuming allowed"

    async def _check_rate_limit(self, domain: str) -> Tuple[bool, str]:
        """Check if domain has exceeded rate limit"""
        try:
            rate_key = f"rate_limit:{domain}"
            current_minute = int(datetime.now().timestamp() // 60)
            rate_window_key = f"{rate_key}:{current_minute}"
            
            current_requests = await self.redis.get(rate_window_key)
            current_requests = int(current_requests) if current_requests else 0
            
            if current_requests >= self.max_requests_per_minute:
                return False, f"rate limit exceeded ({current_requests}/{self.max_requests_per_minute} per minute)"
            
            return True, "rate limit ok"
            
        except Exception as e:
            logger.error(f"Error checking rate limit for {domain}: {e}")
            return True, "rate limit check failed, assuming allowed"

    async def _get_domain_crawl_delay(self, domain: str) -> float:
        """Get crawl delay for specific domain"""
        try:
            # Check if domain has custom crawl delay from robots.txt or configuration
            stats_key = f"{self.domain_stats_key}:{domain}"
            stats_str = await self.redis.get(stats_key)
            
            if stats_str:
                stats = json.loads(stats_str)
                custom_delay = stats.get('crawl_delay')
                if custom_delay:
                    return float(custom_delay)
            
            return self.default_crawl_delay
            
        except Exception as e:
            logger.error(f"Error getting crawl delay for {domain}: {e}")
            return self.default_crawl_delay

    async def register_crawl_start(self, url: str, crawl_id: str) -> bool:
        """Register that a crawl has started for tracking"""
        try:
            domain = self._extract_domain(url)
            
            # Update last crawl time
            last_crawl_key = f"{self.domain_delays_key}:{domain}"
            await self.redis.set(last_crawl_key, datetime.now().isoformat())
            
            # Add to active crawls
            active_key = f"{self.active_crawls_key}:{domain}"
            await self.redis.sadd(active_key, crawl_id)
            await self.redis.expire(active_key, 300)  # 5 minute expiry
            
            # Update rate limiting counter
            rate_key = f"rate_limit:{domain}"
            current_minute = int(datetime.now().timestamp() // 60)
            rate_window_key = f"{rate_key}:{current_minute}"
            await self.redis.incr(rate_window_key)
            await self.redis.expire(rate_window_key, self.rate_limit_window)
            
            logger.debug(f"Registered crawl start for {url} (crawl_id: {crawl_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error registering crawl start for {url}: {e}")
            return False

    async def register_crawl_complete(self, url: str, crawl_id: str, success: bool, response_time: float = 0):
        """Register that a crawl has completed"""
        try:
            domain = self._extract_domain(url)
            
            # Remove from active crawls
            active_key = f"{self.active_crawls_key}:{domain}"
            await self.redis.srem(active_key, crawl_id)
            
            # Update domain statistics
            await self._update_domain_stats(domain, success, response_time)
            
            logger.debug(f"Registered crawl complete for {url} (success: {success})")
            
        except Exception as e:
            logger.error(f"Error registering crawl complete for {url}: {e}")

    async def _update_domain_stats(self, domain: str, success: bool, response_time: float):
        """Update domain crawling statistics"""
        try:
            stats_key = f"{self.domain_stats_key}:{domain}"
            stats_str = await self.redis.get(stats_key)
            
            if stats_str:
                stats = json.loads(stats_str)
            else:
                stats = {
                    'total_requests': 0,
                    'successful_requests': 0,
                    'failed_requests': 0,
                    'avg_response_time': 0,
                    'last_crawl': None,
                    'crawl_delay': self.default_crawl_delay
                }
            
            # Update statistics
            stats['total_requests'] += 1
            if success:
                stats['successful_requests'] += 1
            else:
                stats['failed_requests'] += 1
            
            # Update average response time
            if response_time > 0:
                current_avg = stats['avg_response_time']
                total_requests = stats['total_requests']
                stats['avg_response_time'] = (current_avg * (total_requests - 1) + response_time) / total_requests
            
            stats['last_crawl'] = datetime.now().isoformat()
            
            # Adaptive crawl delay based on response time and success rate
            if stats['total_requests'] >= 10:  # Only adjust after some requests
                success_rate = stats['successful_requests'] / stats['total_requests']
                avg_response = stats['avg_response_time']
                
                if success_rate < 0.8 or avg_response > 5.0:
                    # Increase delay if low success rate or slow responses
                    stats['crawl_delay'] = min(stats['crawl_delay'] * 1.5, 10.0)
                elif success_rate > 0.95 and avg_response < 1.0:
                    # Decrease delay if high success rate and fast responses
                    stats['crawl_delay'] = max(stats['crawl_delay'] * 0.8, 0.5)
            
            # Cache updated stats
            await self.redis.set(stats_key, json.dumps(stats), ex=3600)  # 1 hour TTL
            
        except Exception as e:
            logger.error(f"Error updating domain stats for {domain}: {e}")

    async def get_domain_stats(self, domain: str) -> Dict:
        """Get crawling statistics for a domain"""
        try:
            stats_key = f"{self.domain_stats_key}:{domain}"
            stats_str = await self.redis.get(stats_key)
            
            if stats_str:
                return json.loads(stats_str)
            else:
                return {
                    'total_requests': 0,
                    'successful_requests': 0,
                    'failed_requests': 0,
                    'avg_response_time': 0,
                    'last_crawl': None,
                    'crawl_delay': self.default_crawl_delay
                }
                
        except Exception as e:
            logger.error(f"Error getting domain stats for {domain}: {e}")
            return {}

    async def cleanup_expired_crawls(self):
        """Clean up expired crawl tracking data"""
        try:
            # This is automatically handled by Redis TTL, but we can do additional cleanup
            current_time = datetime.now()
            
            # Clean up very old rate limiting data
            rate_keys = await self.redis.keys("rate_limit:*")
            for key in rate_keys:
                try:
                    # Extract timestamp from key
                    parts = key.decode().split(':')
                    if len(parts) >= 3:
                        timestamp = int(parts[-1])
                        key_time = datetime.fromtimestamp(timestamp * 60)
                        
                        if (current_time - key_time).total_seconds() > self.rate_limit_window * 2:
                            await self.redis.delete(key)
                except Exception:
                    continue
                    
        except Exception as e:
            logger.error(f"Error cleaning up expired crawls: {e}")

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Remove www. prefix
            if domain.startswith('www.'):
                domain = domain[4:]
                
            return domain
        except Exception:
            return "unknown" 