"""
URL Normalization Module

URL normalization is the process of modifying and standardizing URLs in a consistent manner.
This helps with deduplication by ensuring that URLs that point to the same resource
are treated as identical.

Examples of normalization:
- http://example.com/path/ -> https://example.com/path
- http://EXAMPLE.COM/Path -> https://example.com/path  
- http://example.com/path?utm_source=google&id=123 -> https://example.com/path?id=123
"""

import re
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from typing import Optional


class URLNormalizer:
    def __init__(self):
        # Parameters to remove (tracking, session, etc.)
        self.params_to_remove = {
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
            'gclid', 'fbclid', 'msclkid', '_ga', '_gid', 'sessionid', 'jsessionid',
            'phpsessid', 'sid', 'ref', 'referrer'
        }
        
        # File extensions to ignore
        self.ignore_extensions = {
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp',
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.zip', '.rar', '.tar', '.gz', '.mp3', '.mp4', '.avi', '.mov'
        }

    def normalize(self, url: str) -> Optional[str]:
        """
        Normalize a URL for consistent storage and deduplication
        
        Args:
            url: Raw URL string
            
        Returns:
            Normalized URL string or None if URL should be ignored
        """
        if not url or not isinstance(url, str):
            return None
            
        url = url.strip()
        
        # Skip if empty or invalid
        if not url or len(url) < 10:
            return None
            
        # Add protocol if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
            
        try:
            parsed = urlparse(url)
        except Exception:
            return None
            
        # Skip if no domain
        if not parsed.netloc:
            return None
            
        # Skip certain file extensions
        path_lower = parsed.path.lower()
        if any(path_lower.endswith(ext) for ext in self.ignore_extensions):
            return None
            
        # Normalize components
        scheme = self._normalize_scheme(parsed.scheme)
        netloc = self._normalize_netloc(parsed.netloc)
        path = self._normalize_path(parsed.path)
        query = self._normalize_query(parsed.query)
        
        # Reconstruct URL
        normalized = urlunparse((
            scheme,
            netloc,
            path,
            '',  # params (rarely used)
            query,
            ''   # fragment (not needed for crawling)
        ))
        
        return normalized

    def _normalize_scheme(self, scheme: str) -> str:
        """Convert to lowercase, prefer https"""
        scheme = scheme.lower()
        # For normalization, we'll use https as default
        return 'https' if scheme in ('http', 'https') else scheme

    def _normalize_netloc(self, netloc: str) -> str:
        """Normalize domain name"""
        netloc = netloc.lower()
        
        # Remove www. prefix for normalization
        if netloc.startswith('www.'):
            netloc = netloc[4:]
            
        # Remove default ports
        if netloc.endswith(':80'):
            netloc = netloc[:-3]
        elif netloc.endswith(':443'):
            netloc = netloc[:-4]
            
        return netloc

    def _normalize_path(self, path: str) -> str:
        """Normalize URL path"""
        if not path:
            return '/'
            
        # Remove duplicate slashes
        path = re.sub(r'/+', '/', path)
        
        # Remove trailing slash (except for root)
        if len(path) > 1 and path.endswith('/'):
            path = path[:-1]
            
        return path

    def _normalize_query(self, query: str) -> str:
        """Normalize query parameters"""
        if not query:
            return ''
            
        try:
            # Parse query parameters
            params = parse_qs(query, keep_blank_values=False)
            
            # Remove tracking parameters
            filtered_params = {
                k: v for k, v in params.items() 
                if k.lower() not in self.params_to_remove
            }
            
            # Sort parameters for consistent ordering
            sorted_params = sorted(filtered_params.items())
            
            # Rebuild query string
            if sorted_params:
                return urlencode(sorted_params, doseq=True)
            else:
                return ''
                
        except Exception:
            return ''

    def extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Remove www. prefix
            if domain.startswith('www.'):
                domain = domain[4:]
                
            return domain
        except Exception:
            return None

    def get_url_priority(self, url: str, depth: int = 0, source_priority: int = 0) -> int:
        """
        Calculate URL priority for crawling
        Higher number = higher priority
        
        Priority factors:
        - Depth (shallower = higher priority)
        - Domain authority (can be enhanced later)
        - Source priority
        - URL characteristics
        """
        base_priority = 100
        
        # Depth penalty (deeper pages get lower priority)
        depth_penalty = depth * 10
        
        # URL characteristics bonus
        url_lower = url.lower()
        characteristic_bonus = 0
        
        # Homepage and important pages get bonus
        if url_lower.endswith('/') or url_lower.count('/') <= 3:
            characteristic_bonus += 20
            
        # News, blog, article pages get bonus
        if any(keyword in url_lower for keyword in ['news', 'blog', 'article', 'post']):
            characteristic_bonus += 10
            
        # Archive, tag, category pages get penalty
        if any(keyword in url_lower for keyword in ['archive', 'tag', 'category', 'page=']):
            characteristic_bonus -= 15
            
        final_priority = base_priority + source_priority + characteristic_bonus - depth_penalty
        
        return max(1, final_priority)  # Minimum priority of 1 