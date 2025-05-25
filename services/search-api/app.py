#!/usr/bin/env python3
"""
Search API Service

This service provides a comprehensive search interface over the indexed web content.
It offers full-text search, filtering, aggregations, and analytics capabilities.

Features:
- Full-text search with relevance scoring
- Advanced filtering (domain, language, content type, quality score)
- Faceted search with aggregations
- Search suggestions and autocomplete
- Content analytics and statistics
- RESTful API with FastAPI
- Caching for performance
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from urllib.parse import urlparse
import re

from fastapi import FastAPI, HTTPException, Query, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import redis.asyncio as redis
from elasticsearch import AsyncElasticsearch
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Web Crawler Search API",
    description="Search interface for crawled web content",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models for request/response
class SearchRequest(BaseModel):
    query: str = Field(..., description="Search query")
    page: int = Field(1, ge=1, description="Page number")
    size: int = Field(10, ge=1, le=100, description="Results per page")
    domains: Optional[List[str]] = Field(None, description="Filter by domains")
    languages: Optional[List[str]] = Field(None, description="Filter by languages")
    content_types: Optional[List[str]] = Field(None, description="Filter by content types")
    min_quality_score: Optional[float] = Field(None, ge=0, le=100, description="Minimum quality score")
    date_from: Optional[str] = Field(None, description="Date from (ISO format)")
    date_to: Optional[str] = Field(None, description="Date to (ISO format)")
    sort_by: Optional[str] = Field("relevance", description="Sort by: relevance, date, quality")
    include_facets: bool = Field(True, description="Include faceted search results")


class SearchResult(BaseModel):
    url: str
    title: str
    content: str
    description: Optional[str]
    domain: str
    language: str
    content_type: str
    quality_score: float
    crawled_at: str
    indexed_at: str
    score: float


class SearchResponse(BaseModel):
    query: str
    total_hits: int
    page: int
    size: int
    total_pages: int
    results: List[SearchResult]
    facets: Optional[Dict[str, Any]] = None
    suggestions: Optional[List[str]] = None
    search_time_ms: float


class AnalyticsResponse(BaseModel):
    total_documents: int
    total_domains: int
    total_links: int
    content_distribution: Dict[str, int]
    language_distribution: Dict[str, int]
    quality_distribution: Dict[str, int]
    crawl_statistics: Dict[str, Any]


class SuggestionRequest(BaseModel):
    query: str = Field(..., description="Partial query for suggestions")
    size: int = Field(5, ge=1, le=20, description="Number of suggestions")


# Global variables for services
elasticsearch_client = None
redis_client = None
search_cache = {}


class SearchService:
    """Main search service with Elasticsearch integration"""
    
    def __init__(self, elasticsearch_url: str, redis_url: str):
        self.elasticsearch_url = elasticsearch_url
        self.redis_url = redis_url
        self.es_client = None
        self.redis_client = None
        
        # Index names
        self.content_index = "web_content"
        self.links_index = "web_links"
        self.domains_index = "web_domains"
        
        # Cache settings
        self.cache_ttl = 300  # 5 minutes
        self.suggestion_cache_ttl = 3600  # 1 hour
        
    async def initialize(self):
        """Initialize Elasticsearch and Redis connections"""
        try:
            # Initialize Elasticsearch
            self.es_client = AsyncElasticsearch([self.elasticsearch_url])
            await self.es_client.ping()
            logger.info("Connected to Elasticsearch")
            
            # Initialize Redis
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            await self.redis_client.ping()
            logger.info("Connected to Redis")
            
        except Exception as e:
            logger.error(f"Failed to initialize search service: {e}")
            raise
    
    async def search_content(self, request: SearchRequest) -> SearchResponse:
        """Perform content search with filtering and faceting"""
        start_time = time.time()
        
        # Check cache first
        cache_key = self._generate_cache_key(request)
        cached_result = await self._get_cached_result(cache_key)
        if cached_result:
            return cached_result
        
        # Build Elasticsearch query
        es_query = self._build_search_query(request)
        
        try:
            # Execute search
            response = await self.es_client.search(
                index=self.content_index,
                body=es_query,
                request_timeout=30
            )
            
            # Process results
            search_response = await self._process_search_response(response, request, start_time)
            
            # Cache results
            await self._cache_result(cache_key, search_response)
            
            return search_response
            
        except Exception as e:
            logger.error(f"Search error: {e}")
            raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
    
    def _build_search_query(self, request: SearchRequest) -> Dict:
        """Build Elasticsearch query from search request"""
        query = {
            "size": request.size,
            "from": (request.page - 1) * request.size,
            "_source": [
                "url", "title", "content", "description", "domain", 
                "language", "content_type", "quality_score", 
                "crawled_at", "indexed_at"
            ]
        }
        
        # Build main query
        if request.query.strip():
            # Multi-match query with boosting
            main_query = {
                "multi_match": {
                    "query": request.query,
                    "fields": [
                        "title^3",      # Boost title matches
                        "description^2", # Boost description matches
                        "content",      # Standard content matches
                        "keywords^1.5"  # Boost keyword matches
                    ],
                    "type": "best_fields",
                    "fuzziness": "AUTO",
                    "operator": "and"
                }
            }
        else:
            main_query = {"match_all": {}}
        
        # Build filters
        filters = []
        
        if request.domains:
            filters.append({"terms": {"domain": request.domains}})
        
        if request.languages:
            filters.append({"terms": {"language": request.languages}})
        
        if request.content_types:
            filters.append({"terms": {"content_type": request.content_types}})
        
        if request.min_quality_score is not None:
            filters.append({"range": {"quality_score": {"gte": request.min_quality_score}}})
        
        # Date range filter
        if request.date_from or request.date_to:
            date_filter = {"range": {"crawled_at": {}}}
            if request.date_from:
                date_filter["range"]["crawled_at"]["gte"] = request.date_from
            if request.date_to:
                date_filter["range"]["crawled_at"]["lte"] = request.date_to
            filters.append(date_filter)
        
        # Combine query and filters
        if filters:
            query["query"] = {
                "bool": {
                    "must": [main_query],
                    "filter": filters
                }
            }
        else:
            query["query"] = main_query
        
        # Add sorting
        if request.sort_by == "date":
            query["sort"] = [{"crawled_at": {"order": "desc"}}]
        elif request.sort_by == "quality":
            query["sort"] = [{"quality_score": {"order": "desc"}}]
        # Default is relevance (Elasticsearch _score)
        
        # Add facets/aggregations
        if request.include_facets:
            query["aggs"] = {
                "domains": {
                    "terms": {"field": "domain", "size": 20}
                },
                "languages": {
                    "terms": {"field": "language", "size": 10}
                },
                "content_types": {
                    "terms": {"field": "content_type", "size": 10}
                },
                "quality_ranges": {
                    "range": {
                        "field": "quality_score",
                        "ranges": [
                            {"from": 0, "to": 25, "key": "low"},
                            {"from": 25, "to": 50, "key": "medium"},
                            {"from": 50, "to": 75, "key": "good"},
                            {"from": 75, "to": 100, "key": "excellent"}
                        ]
                    }
                }
            }
        
        return query
    
    async def _process_search_response(self, response: Dict, request: SearchRequest, start_time: float) -> SearchResponse:
        """Process Elasticsearch response into SearchResponse"""
        hits = response["hits"]
        total_hits = hits["total"]["value"]
        total_pages = (total_hits + request.size - 1) // request.size
        
        # Process search results
        results = []
        for hit in hits["hits"]:
            source = hit["_source"]
            
            # Truncate content for display
            content = source.get("content", "")
            if len(content) > 500:
                content = content[:500] + "..."
            
            result = SearchResult(
                url=source.get("url", ""),
                title=source.get("title", ""),
                content=content,
                description=source.get("description"),
                domain=source.get("domain", ""),
                language=source.get("language", ""),
                content_type=source.get("content_type", ""),
                quality_score=source.get("quality_score", 0),
                crawled_at=source.get("crawled_at", ""),
                indexed_at=source.get("indexed_at", ""),
                score=hit["_score"]
            )
            results.append(result)
        
        # Process facets
        facets = None
        if request.include_facets and "aggregations" in response:
            facets = self._process_facets(response["aggregations"])
        
        # Generate suggestions (simplified)
        suggestions = await self._generate_suggestions(request.query) if request.query else None
        
        search_time_ms = (time.time() - start_time) * 1000
        
        return SearchResponse(
            query=request.query,
            total_hits=total_hits,
            page=request.page,
            size=request.size,
            total_pages=total_pages,
            results=results,
            facets=facets,
            suggestions=suggestions,
            search_time_ms=search_time_ms
        )
    
    def _process_facets(self, aggregations: Dict) -> Dict[str, Any]:
        """Process Elasticsearch aggregations into facets"""
        facets = {}
        
        for agg_name, agg_data in aggregations.items():
            if "buckets" in agg_data:
                facets[agg_name] = [
                    {"key": bucket["key"], "count": bucket["doc_count"]}
                    for bucket in agg_data["buckets"]
                ]
        
        return facets
    
    async def _generate_suggestions(self, query: str) -> List[str]:
        """Generate search suggestions based on query"""
        if not query or len(query) < 2:
            return []
        
        try:
            # Use Elasticsearch completion suggester
            suggest_query = {
                "suggest": {
                    "title_suggest": {
                        "prefix": query,
                        "completion": {
                            "field": "title.suggest",
                            "size": 5
                        }
                    }
                }
            }
            
            response = await self.es_client.search(
                index=self.content_index,
                body=suggest_query
            )
            
            suggestions = []
            if "suggest" in response:
                for suggestion in response["suggest"]["title_suggest"]:
                    for option in suggestion["options"]:
                        suggestions.append(option["text"])
            
            return suggestions[:5]
            
        except Exception as e:
            logger.warning(f"Suggestion generation failed: {e}")
            return []
    
    async def get_analytics(self) -> AnalyticsResponse:
        """Get content analytics and statistics"""
        try:
            # Get total document counts
            content_count = await self.es_client.count(index=self.content_index)
            links_count = await self.es_client.count(index=self.links_index)
            
            # Get domain count
            domain_agg = await self.es_client.search(
                index=self.content_index,
                body={
                    "size": 0,
                    "aggs": {
                        "unique_domains": {"cardinality": {"field": "domain"}}
                    }
                }
            )
            
            # Get content distribution
            content_dist = await self.es_client.search(
                index=self.content_index,
                body={
                    "size": 0,
                    "aggs": {
                        "content_types": {"terms": {"field": "content_type", "size": 20}},
                        "languages": {"terms": {"field": "language", "size": 20}},
                        "quality_stats": {"stats": {"field": "quality_score"}}
                    }
                }
            )
            
            # Process aggregations
            aggs = content_dist["aggregations"]
            
            content_distribution = {
                bucket["key"]: bucket["doc_count"]
                for bucket in aggs["content_types"]["buckets"]
            }
            
            language_distribution = {
                bucket["key"]: bucket["doc_count"]
                for bucket in aggs["languages"]["buckets"]
            }
            
            quality_stats = aggs["quality_stats"]
            quality_distribution = {
                "average": round(quality_stats["avg"], 2),
                "min": round(quality_stats["min"], 2),
                "max": round(quality_stats["max"], 2),
                "count": quality_stats["count"]
            }
            
            crawl_statistics = {
                "avg_quality_score": round(quality_stats["avg"], 2),
                "total_content_indexed": content_count["count"],
                "total_links_discovered": links_count["count"]
            }
            
            return AnalyticsResponse(
                total_documents=content_count["count"],
                total_domains=domain_agg["aggregations"]["unique_domains"]["value"],
                total_links=links_count["count"],
                content_distribution=content_distribution,
                language_distribution=language_distribution,
                quality_distribution=quality_distribution,
                crawl_statistics=crawl_statistics
            )
            
        except Exception as e:
            logger.error(f"Analytics error: {e}")
            raise HTTPException(status_code=500, detail=f"Analytics failed: {str(e)}")
    
    def _generate_cache_key(self, request: SearchRequest) -> str:
        """Generate cache key for search request"""
        key_data = {
            "query": request.query,
            "page": request.page,
            "size": request.size,
            "domains": request.domains,
            "languages": request.languages,
            "content_types": request.content_types,
            "min_quality_score": request.min_quality_score,
            "date_from": request.date_from,
            "date_to": request.date_to,
            "sort_by": request.sort_by
        }
        return f"search:{hash(json.dumps(key_data, sort_keys=True))}"
    
    async def _get_cached_result(self, cache_key: str) -> Optional[SearchResponse]:
        """Get cached search result"""
        try:
            cached_data = await self.redis_client.get(cache_key)
            if cached_data:
                return SearchResponse.parse_raw(cached_data)
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
        return None
    
    async def _cache_result(self, cache_key: str, result: SearchResponse):
        """Cache search result"""
        try:
            await self.redis_client.setex(
                cache_key,
                self.cache_ttl,
                result.json()
            )
        except Exception as e:
            logger.warning(f"Cache storage error: {e}")
    
    async def close(self):
        """Close connections"""
        if self.es_client:
            await self.es_client.close()
        if self.redis_client:
            await self.redis_client.close()


# Initialize search service
search_service = None


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global search_service
    
    elasticsearch_url = os.getenv('ELASTICSEARCH_URL', 'http://localhost:9200')
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    
    search_service = SearchService(elasticsearch_url, redis_url)
    await search_service.initialize()
    
    logger.info("Search API service started")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global search_service
    
    if search_service:
        await search_service.close()
    
    logger.info("Search API service stopped")


# API Endpoints

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "search-api", "timestamp": datetime.now().isoformat()}


@app.post("/search", response_model=SearchResponse)
async def search_content(request: SearchRequest):
    """Search web content with advanced filtering and faceting"""
    return await search_service.search_content(request)


@app.get("/search", response_model=SearchResponse)
async def search_content_get(
    q: str = Query(..., description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(10, ge=1, le=100, description="Results per page"),
    domains: Optional[str] = Query(None, description="Comma-separated domains"),
    languages: Optional[str] = Query(None, description="Comma-separated languages"),
    content_types: Optional[str] = Query(None, description="Comma-separated content types"),
    min_quality: Optional[float] = Query(None, ge=0, le=100, description="Minimum quality score"),
    date_from: Optional[str] = Query(None, description="Date from (ISO format)"),
    date_to: Optional[str] = Query(None, description="Date to (ISO format)"),
    sort_by: str = Query("relevance", description="Sort by: relevance, date, quality"),
    include_facets: bool = Query(True, description="Include faceted search results")
):
    """Search web content via GET request"""
    request = SearchRequest(
        query=q,
        page=page,
        size=size,
        domains=domains.split(',') if domains else None,
        languages=languages.split(',') if languages else None,
        content_types=content_types.split(',') if content_types else None,
        min_quality_score=min_quality,
        date_from=date_from,
        date_to=date_to,
        sort_by=sort_by,
        include_facets=include_facets
    )
    return await search_service.search_content(request)


@app.post("/suggest")
async def get_suggestions(request: SuggestionRequest):
    """Get search suggestions for autocomplete"""
    suggestions = await search_service._generate_suggestions(request.query)
    return {"query": request.query, "suggestions": suggestions[:request.size]}


@app.get("/analytics", response_model=AnalyticsResponse)
async def get_analytics():
    """Get content analytics and statistics"""
    return await search_service.get_analytics()


@app.get("/domains")
async def get_domains(limit: int = Query(50, ge=1, le=1000)):
    """Get list of crawled domains"""
    try:
        response = await search_service.es_client.search(
            index=search_service.content_index,
            body={
                "size": 0,
                "aggs": {
                    "domains": {
                        "terms": {"field": "domain", "size": limit}
                    }
                }
            }
        )
        
        domains = [
            {
                "domain": bucket["key"],
                "document_count": bucket["doc_count"]
            }
            for bucket in response["aggregations"]["domains"]["buckets"]
        ]
        
        return {"domains": domains, "total_domains": len(domains)}
        
    except Exception as e:
        logger.error(f"Domains endpoint error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get domains: {str(e)}")


@app.get("/content/{content_hash}")
async def get_content_by_hash(content_hash: str):
    """Get specific content by hash"""
    try:
        response = await search_service.es_client.get(
            index=search_service.content_index,
            id=content_hash
        )
        
        return response["_source"]
        
    except Exception as e:
        logger.error(f"Content retrieval error: {e}")
        raise HTTPException(status_code=404, detail="Content not found")


@app.get("/stats")
async def get_system_stats():
    """Get system statistics"""
    try:
        # Get Elasticsearch cluster health
        cluster_health = await search_service.es_client.cluster.health()
        
        # Get index statistics
        content_stats = await search_service.es_client.indices.stats(index=search_service.content_index)
        links_stats = await search_service.es_client.indices.stats(index=search_service.links_index)
        
        return {
            "elasticsearch": {
                "cluster_status": cluster_health["status"],
                "number_of_nodes": cluster_health["number_of_nodes"],
                "content_index": {
                    "documents": content_stats["indices"][search_service.content_index]["total"]["docs"]["count"],
                    "size_bytes": content_stats["indices"][search_service.content_index]["total"]["store"]["size_in_bytes"]
                },
                "links_index": {
                    "documents": links_stats["indices"][search_service.links_index]["total"]["docs"]["count"],
                    "size_bytes": links_stats["indices"][search_service.links_index]["total"]["store"]["size_in_bytes"]
                }
            },
            "cache": {
                "redis_connected": await search_service.redis_client.ping()
            }
        }
        
    except Exception as e:
        logger.error(f"Stats error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    ) 