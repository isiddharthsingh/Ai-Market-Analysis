-- sql/create_tables.sql
-- Reddit data table
CREATE TABLE IF NOT EXISTS reddit_posts (
    id VARCHAR(50),
    run_date DATE DEFAULT CURRENT_DATE,
    title TEXT,
    selftext TEXT,
    score INTEGER,
    num_comments INTEGER,
    created_utc TIMESTAMP,
    subreddit VARCHAR(100),
    author VARCHAR(100),
    url TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sentiment_score FLOAT,
    ai_related BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (id, run_date)
);

-- GitHub repositories table
CREATE TABLE IF NOT EXISTS github_repos (
    id BIGINT,
    run_date DATE DEFAULT CURRENT_DATE,
    name VARCHAR(255),
    full_name VARCHAR(255),
    description TEXT,
    stars INTEGER,
    forks INTEGER,
    watchers INTEGER,
    language VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    size_kb INTEGER,
    open_issues INTEGER,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ai_related BOOLEAN DEFAULT FALSE,
    topics TEXT[],
    PRIMARY KEY (id, run_date)
);

-- Cryptocurrency data table
CREATE TABLE IF NOT EXISTS crypto_data (
    id VARCHAR(100),
    run_date DATE DEFAULT CURRENT_DATE,
    symbol VARCHAR(20),
    name VARCHAR(255),
    current_price DECIMAL(20,8),
    market_cap BIGINT,
    market_cap_rank INTEGER,
    volume_24h BIGINT,
    price_change_24h DECIMAL(10,4),
    price_change_7d DECIMAL(10,4),
    last_updated TIMESTAMP,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ai_related BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (id, run_date)
);

-- Correlation analysis results
CREATE TABLE IF NOT EXISTS correlation_analysis (
    analysis_date DATE PRIMARY KEY,
    reddit_ai_posts_count INTEGER,
    github_ai_repos_count INTEGER,
    crypto_ai_projects_count INTEGER,
    reddit_github_correlation DECIMAL(5,4),
    reddit_crypto_correlation DECIMAL(5,4),
    github_crypto_correlation DECIMAL(5,4),
    analysis_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data quality metrics
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,4),
    threshold_value DECIMAL(10,4),
    status VARCHAR(20),
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_reddit_created_utc ON reddit_posts(created_utc);
CREATE INDEX IF NOT EXISTS idx_reddit_ai_related ON reddit_posts(ai_related);
CREATE INDEX IF NOT EXISTS idx_github_created_at ON github_repos(created_at);
CREATE INDEX IF NOT EXISTS idx_github_ai_related ON github_repos(ai_related);
CREATE INDEX IF NOT EXISTS idx_crypto_last_updated ON crypto_data(last_updated);
CREATE INDEX IF NOT EXISTS idx_correlation_analysis_date ON correlation_analysis(analysis_date);