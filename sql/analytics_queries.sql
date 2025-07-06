-- Top AI-related Reddit posts by engagement
SELECT 
    title,
    score,
    num_comments,
    sentiment_score,
    subreddit,
    created_utc
FROM reddit_posts 
WHERE ai_related = true
ORDER BY (score + num_comments) DESC
LIMIT 20;

-- Most popular AI programming languages on GitHub
SELECT 
    language,
    COUNT(*) as repo_count,
    AVG(stars) as avg_stars,
    SUM(stars) as total_stars
FROM github_repos 
WHERE ai_related = true 
AND language IS NOT NULL
GROUP BY language
ORDER BY repo_count DESC;

-- AI crypto projects performance
SELECT 
    c.name,
    c.symbol,
    c.current_price,
    c.price_change_24h,
    c.market_cap_rank
FROM crypto_data c
WHERE c.ai_related = true
ORDER BY c.market_cap_rank ASC;

-- Weekly correlation trends
SELECT 
    DATE_TRUNC('week', analysis_date) as week,
    AVG(reddit_ai_posts_count) as avg_reddit_posts,
    AVG(github_ai_repos_count) as avg_github_repos,
    AVG(crypto_ai_projects_count) as avg_crypto_projects,
    AVG(reddit_github_correlation) as avg_reddit_github_corr
FROM correlation_analysis
GROUP BY DATE_TRUNC('week', analysis_date)
ORDER BY week DESC;