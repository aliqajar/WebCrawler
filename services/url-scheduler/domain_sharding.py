"""
Domain Sharding Module

This module implements domain sharding strategies to distribute crawling load:
1. Round-robin distribution across fetchers
2. Load-based distribution
3. Domain-specific fetcher assignment
4. Geographic distribution (future enhancement)
"""

import hashlib
import json
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import aioredis

logger = logging.getLogger(__name__)


class DomainSharding:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client
        
        # Sharding configuration
        self.num_fetcher_shards = 4  # Number of fetcher instances
        self.max_urls_per_shard_per_minute = 100
        self.rebalance_interval = 300  # 5 minutes
        
        # Redis keys
        self.shard_assignments_key = "shard_assignments"
        self.shard_loads_key = "shard_loads"
        self.domain_preferences_key = "domain_preferences"
        
        # Sharding strategies
        self.strategies = {
            'round_robin': self._round_robin_shard,
            'load_balanced': self._load_balanced_shard,
            'hash_based': self._hash_based_shard,
            'domain_sticky': self._domain_sticky_shard
        }
        
        self.default_strategy = 'load_balanced'

    async def assign_shard(self, domain: str, url: str, strategy: str = None) -> Tuple[int, str]:
        """
        Assign a shard (fetcher instance) for the given domain/URL
        
        Returns:
            (shard_id, assignment_reason)
        """
        strategy = strategy or self.default_strategy
        
        if strategy not in self.strategies:
            logger.warning(f"Unknown sharding strategy: {strategy}, using default")
            strategy = self.default_strategy
        
        try:
            shard_id = await self.strategies[strategy](domain, url)
            
            # Update shard load tracking
            await self._update_shard_load(shard_id, domain)
            
            return shard_id, f"assigned by {strategy} strategy"
            
        except Exception as e:
            logger.error(f"Error assigning shard for {domain}: {e}")
            # Fallback to simple hash-based assignment
            shard_id = await self._hash_based_shard(domain, url)
            return shard_id, "assigned by fallback hash strategy"

    async def _round_robin_shard(self, domain: str, url: str) -> int:
        """Simple round-robin assignment"""
        try:
            # Get current round-robin counter
            counter_key = "round_robin_counter"
            current = await self.redis.get(counter_key)
            current = int(current) if current else 0
            
            # Increment and wrap around
            next_shard = (current + 1) % self.num_fetcher_shards
            await self.redis.set(counter_key, next_shard)
            
            return next_shard
            
        except Exception as e:
            logger.error(f"Error in round-robin sharding: {e}")
            return 0

    async def _load_balanced_shard(self, domain: str, url: str) -> int:
        """Load-balanced assignment based on current shard loads"""
        try:
            # Get current loads for all shards
            shard_loads = await self._get_all_shard_loads()
            
            # Find shard with minimum load
            min_load = float('inf')
            best_shard = 0
            
            for shard_id in range(self.num_fetcher_shards):
                load = shard_loads.get(shard_id, 0)
                if load < min_load:
                    min_load = load
                    best_shard = shard_id
            
            return best_shard
            
        except Exception as e:
            logger.error(f"Error in load-balanced sharding: {e}")
            return await self._hash_based_shard(domain, url)

    async def _hash_based_shard(self, domain: str, url: str) -> int:
        """Consistent hash-based assignment"""
        try:
            # Use domain for consistent assignment
            domain_hash = hashlib.md5(domain.encode()).hexdigest()
            shard_id = int(domain_hash, 16) % self.num_fetcher_shards
            return shard_id
            
        except Exception as e:
            logger.error(f"Error in hash-based sharding: {e}")
            return 0

    async def _domain_sticky_shard(self, domain: str, url: str) -> int:
        """Domain-sticky assignment - same domain always goes to same shard"""
        try:
            # Check if domain already has an assigned shard
            assignment_key = f"{self.shard_assignments_key}:{domain}"
            assigned_shard = await self.redis.get(assignment_key)
            
            if assigned_shard is not None:
                return int(assigned_shard)
            
            # No existing assignment, use load-balanced assignment
            shard_id = await self._load_balanced_shard(domain, url)
            
            # Store the assignment for future use
            await self.redis.set(assignment_key, shard_id, ex=3600)  # 1 hour TTL
            
            return shard_id
            
        except Exception as e:
            logger.error(f"Error in domain-sticky sharding: {e}")
            return await self._hash_based_shard(domain, url)

    async def _get_all_shard_loads(self) -> Dict[int, int]:
        """Get current load for all shards"""
        try:
            loads = {}
            current_minute = int(datetime.now().timestamp() // 60)
            
            for shard_id in range(self.num_fetcher_shards):
                load_key = f"{self.shard_loads_key}:{shard_id}:{current_minute}"
                load = await self.redis.get(load_key)
                loads[shard_id] = int(load) if load else 0
            
            return loads
            
        except Exception as e:
            logger.error(f"Error getting shard loads: {e}")
            return {}

    async def _update_shard_load(self, shard_id: int, domain: str):
        """Update load tracking for a shard"""
        try:
            current_minute = int(datetime.now().timestamp() // 60)
            
            # Update per-minute load counter
            load_key = f"{self.shard_loads_key}:{shard_id}:{current_minute}"
            await self.redis.incr(load_key)
            await self.redis.expire(load_key, 120)  # 2 minute TTL
            
            # Update domain assignment tracking
            domain_key = f"{self.shard_assignments_key}:{domain}"
            await self.redis.set(domain_key, shard_id, ex=3600)  # 1 hour TTL
            
        except Exception as e:
            logger.error(f"Error updating shard load: {e}")

    async def get_shard_statistics(self) -> Dict:
        """Get comprehensive shard statistics"""
        try:
            stats = {
                'total_shards': self.num_fetcher_shards,
                'current_loads': await self._get_all_shard_loads(),
                'domain_assignments': {},
                'load_distribution': {}
            }
            
            # Get domain assignments
            assignment_keys = await self.redis.keys(f"{self.shard_assignments_key}:*")
            for key in assignment_keys:
                try:
                    domain = key.decode().split(':')[-1]
                    shard_id = await self.redis.get(key)
                    if shard_id:
                        stats['domain_assignments'][domain] = int(shard_id)
                except Exception:
                    continue
            
            # Calculate load distribution
            total_load = sum(stats['current_loads'].values())
            if total_load > 0:
                for shard_id, load in stats['current_loads'].items():
                    stats['load_distribution'][shard_id] = (load / total_load) * 100
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting shard statistics: {e}")
            return {}

    async def rebalance_shards(self) -> Dict:
        """Rebalance shard assignments based on current loads"""
        try:
            stats = await self.get_shard_statistics()
            current_loads = stats['current_loads']
            
            # Calculate load imbalance
            if not current_loads:
                return {'rebalanced': False, 'reason': 'no load data'}
            
            max_load = max(current_loads.values())
            min_load = min(current_loads.values())
            imbalance_ratio = max_load / max(min_load, 1)
            
            # Only rebalance if significant imbalance
            if imbalance_ratio < 2.0:
                return {'rebalanced': False, 'reason': 'load balanced', 'imbalance_ratio': imbalance_ratio}
            
            # Find overloaded and underloaded shards
            avg_load = sum(current_loads.values()) / len(current_loads)
            overloaded_shards = [sid for sid, load in current_loads.items() if load > avg_load * 1.5]
            underloaded_shards = [sid for sid, load in current_loads.items() if load < avg_load * 0.5]
            
            if not overloaded_shards or not underloaded_shards:
                return {'rebalanced': False, 'reason': 'no clear rebalancing targets'}
            
            # Move some domain assignments from overloaded to underloaded shards
            rebalanced_domains = []
            
            for overloaded_shard in overloaded_shards:
                if not underloaded_shards:
                    break
                
                # Find domains assigned to this overloaded shard
                domains_to_move = []
                for domain, assigned_shard in stats['domain_assignments'].items():
                    if assigned_shard == overloaded_shard:
                        domains_to_move.append(domain)
                
                # Move some domains to underloaded shards
                for i, domain in enumerate(domains_to_move[:2]):  # Move max 2 domains per rebalance
                    if underloaded_shards:
                        target_shard = underloaded_shards.pop(0)
                        
                        # Update assignment
                        assignment_key = f"{self.shard_assignments_key}:{domain}"
                        await self.redis.set(assignment_key, target_shard, ex=3600)
                        
                        rebalanced_domains.append({
                            'domain': domain,
                            'from_shard': overloaded_shard,
                            'to_shard': target_shard
                        })
            
            return {
                'rebalanced': True,
                'domains_moved': len(rebalanced_domains),
                'details': rebalanced_domains,
                'imbalance_ratio': imbalance_ratio
            }
            
        except Exception as e:
            logger.error(f"Error rebalancing shards: {e}")
            return {'rebalanced': False, 'reason': f'error: {e}'}

    async def get_fetcher_queue_name(self, shard_id: int) -> str:
        """Get the Kafka topic/queue name for a specific fetcher shard"""
        return f"fetch_queue_shard_{shard_id}"

    async def is_shard_overloaded(self, shard_id: int) -> bool:
        """Check if a shard is currently overloaded"""
        try:
            current_loads = await self._get_all_shard_loads()
            current_load = current_loads.get(shard_id, 0)
            
            return current_load >= self.max_urls_per_shard_per_minute
            
        except Exception as e:
            logger.error(f"Error checking shard overload: {e}")
            return False

    async def cleanup_old_assignments(self):
        """Clean up old shard assignments and load data"""
        try:
            current_time = datetime.now()
            
            # Clean up old load tracking data
            load_keys = await self.redis.keys(f"{self.shard_loads_key}:*")
            for key in load_keys:
                try:
                    # Extract timestamp from key
                    parts = key.decode().split(':')
                    if len(parts) >= 3:
                        timestamp = int(parts[-1])
                        key_time = datetime.fromtimestamp(timestamp * 60)
                        
                        # Remove data older than 10 minutes
                        if (current_time - key_time).total_seconds() > 600:
                            await self.redis.delete(key)
                except Exception:
                    continue
            
            logger.debug("Cleaned up old shard assignment data")
            
        except Exception as e:
            logger.error(f"Error cleaning up old assignments: {e}")

    def get_recommended_fetcher_count(self, expected_urls_per_minute: int) -> int:
        """Get recommended number of fetcher instances based on expected load"""
        # Assume each fetcher can handle ~100 URLs per minute efficiently
        urls_per_fetcher = 100
        recommended = max(1, (expected_urls_per_minute + urls_per_fetcher - 1) // urls_per_fetcher)
        
        # Cap at reasonable maximum
        return min(recommended, 16) 