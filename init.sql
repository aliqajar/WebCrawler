-- URL Frontier Database Schema

-- Main URLs table with priority and metadata
CREATE TABLE IF NOT EXISTS urls (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    url_hash TEXT UNIQUE NOT NULL,
    domain TEXT NOT NULL,
    priority INTEGER DEFAULT 0,
    depth INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    last_crawled TIMESTAMP,
    crawl_count INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    source_url TEXT,
    content_type TEXT,
    UNIQUE(url_hash)
);

-- URL priority queue - separate table for efficient priority-based retrieval
CREATE TABLE IF NOT EXISTS url_queue (
    id SERIAL PRIMARY KEY,
    url_id INTEGER REFERENCES urls(id) ON DELETE CASCADE,
    priority INTEGER NOT NULL,
    domain TEXT NOT NULL,
    queued_at TIMESTAMP DEFAULT NOW(),
    scheduled_for TIMESTAMP DEFAULT NOW()
);

-- Domain statistics for load balancing
CREATE TABLE IF NOT EXISTS domain_stats (
    domain TEXT PRIMARY KEY,
    total_urls INTEGER DEFAULT 0,
    crawled_urls INTEGER DEFAULT 0,
    failed_urls INTEGER DEFAULT 0,
    last_crawl TIMESTAMP,
    avg_response_time FLOAT DEFAULT 0,
    robots_txt TEXT,
    crawl_delay INTEGER DEFAULT 1
);

-- Crawl history for analytics
CREATE TABLE IF NOT EXISTS crawl_history (
    id SERIAL PRIMARY KEY,
    url_id INTEGER REFERENCES urls(id),
    crawled_at TIMESTAMP DEFAULT NOW(),
    status_code INTEGER,
    response_time FLOAT,
    content_length INTEGER,
    success BOOLEAN DEFAULT FALSE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_urls_domain ON urls(domain);
CREATE INDEX IF NOT EXISTS idx_urls_priority ON urls(priority DESC);
CREATE INDEX IF NOT EXISTS idx_urls_status ON urls(status);
CREATE INDEX IF NOT EXISTS idx_urls_normalized ON urls(normalized_url);
CREATE INDEX IF NOT EXISTS idx_url_queue_priority ON url_queue(priority DESC, scheduled_for ASC);
CREATE INDEX IF NOT EXISTS idx_url_queue_domain ON url_queue(domain);
CREATE INDEX IF NOT EXISTS idx_domain_stats_domain ON domain_stats(domain);

-- Function to update domain stats
CREATE OR REPLACE FUNCTION update_domain_stats()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO domain_stats (domain, total_urls)
    VALUES (NEW.domain, 1)
    ON CONFLICT (domain)
    DO UPDATE SET total_urls = domain_stats.total_urls + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update domain stats
CREATE TRIGGER trigger_update_domain_stats
    AFTER INSERT ON urls
    FOR EACH ROW
    EXECUTE FUNCTION update_domain_stats(); 