# Data Processor & AI Trends Analysis Pipeline

This project is a fully containerized data engineering pipeline built with Apache Airflow. It extracts data from Reddit, GitHub, and CoinGecko to analyze trends and correlations in the AI, machine learning, and cryptocurrency ecosystems.

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Setup and Installation](#setup-and-installation)
- [The ETL and Analytics DAGs](#the-etl-and-analytics-dags)
- [Running Analytics Queries](#running-analytics-queries)
- [Example Insights](#example-insights)
- [How to Use the Pipeline](#how-to-use-the-pipeline)
- [Future Improvements](#future-improvements)

## Project Overview

The core goal of this project is to answer questions like:
- Is there a correlation between social media hype and development activity in AI?
- What are the most popular AI projects on GitHub right now?
- What is the overall sentiment of the public conversation around AI?
- How do trends in AI-related crypto projects align with developer and social media activity?

To achieve this, the system automates the process of fetching data from multiple sources, loading it into a central data warehouse, and running analytical queries on it.

## Architecture

The entire project runs within Docker containers, orchestrated by `docker-compose`. This ensures a consistent and reproducible environment.

- **Orchestration:** Apache Airflow
- **Executor:** Celery Executor (for parallel task execution)
- **Message Broker:** Redis
- **Data Warehouse:** PostgreSQL
- **Core Technologies:** Python, Pandas, SQLAlchemy

The setup includes:
- An Airflow webserver to access the UI.
- An Airflow scheduler to trigger and manage DAG runs.
- Airflow workers to execute the tasks.
- A PostgreSQL database to store the data.
- A Redis instance for the Celery message queue.

## Setup and Installation

Follow these steps to get the pipeline running on your local machine.

### Prerequisites
- Docker
- Docker Compose

### 1. Create an Environment File
Create a file named `.env` in the root of the project directory. This file will store your secrets and environment-specific configurations.

```bash
# .env
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
AIRFLOW_FERNET_KEY=$(openssl rand -hex 32)
SECRET_KEY=$(openssl rand -hex 32)

# Generate these keys using: openssl rand -hex 32
AIRFLOW_FERNET_KEY=your_generated_fernet_key_here
SECRET_KEY=your_generated_secret_key_here

# Optional: Add your GitHub token for higher API rate limits
# GITHUB_TOKEN=your_github_personal_access_token

# Airflow Configuration
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
```

**Important Security Notes:**
- Generate unique keys using `openssl rand -hex 32` for both `AIRFLOW_FERNET_KEY` and `SECRET_KEY`
- Never commit the `.env` file to version control
- For production, use a secure secrets management system

### 2. Build and Start the Containers
From the root of the project directory, run the following command:
```bash
docker-compose up --build -d
```
This will build the custom Airflow image, download the Postgres and Redis images, and start all the services in the background.

**Verify containers are running:**
```bash
# Check container status
docker-compose ps

# View logs if needed
docker-compose logs
docker-compose logs airflow-webserver
```

**System Requirements:**
- Minimum: 4GB RAM, 2 CPU cores, 10GB disk space
- Recommended: 8GB RAM, 4 CPU cores, 20GB disk space

### 3. Access the Airflow UI
Once the containers are running, you can access the Airflow web UI at:
- **URL:** `http://localhost:8080`
- **Username:** `admin`
- **Password:** `admin`

**Note:** For production deployments, change these default credentials immediately.

### 4. Configure Airflow Connections
In the Airflow UI, go to **Admin -> Connections** and ensure the `postgres_default` connection is configured correctly to use the `airflow` user, not the default `postgres` user.

- **Connection ID:** `postgres_default`
- **Connection Type:** `Postgres`
- **Host:** `postgres`
- **Schema:** `airflow`
- **Login:** `airflow`
- **Password:** `airflow`
- **Port:** `5432`

**Troubleshooting Connection Issues:**
- If the connection test fails, delete the existing `postgres_default` connection and create a new one
- Ensure you're using `airflow` as both username and password, not `postgres`
- Test the connection before saving

### 5. Enable and Start DAGs
The DAGs are paused by default. In the Airflow UI:

1. **Enable DAGs:** Toggle ON the three DAGs on the main page:
   - `social_media_analytics_pipeline`
   - `advanced_correlation_analysis`
   - `data_monitoring_pipeline`

2. **Trigger First Run:** Click the "play" button (▶) on `social_media_analytics_pipeline` to start the initial data collection

3. **Monitor Progress:** 
   - Click on a DAG to see the Graph View
   - Click on individual tasks to view logs
   - Initial run takes 5-10 minutes to complete

### 6. Verify Data Collection
After the first successful run, verify data was collected:

```bash
# Connect to database
docker-compose exec postgres psql -U airflow -d airflow

# Check data collection (run these in psql)
SELECT COUNT(*) FROM reddit_posts WHERE DATE(extracted_at) = CURRENT_DATE;
SELECT COUNT(*) FROM github_repos WHERE DATE(extracted_at) = CURRENT_DATE;
SELECT COUNT(*) FROM crypto_data WHERE DATE(extracted_at) = CURRENT_DATE;

# Check data quality metrics
SELECT * FROM data_quality_metrics ORDER BY check_timestamp DESC LIMIT 10;

# Exit psql
\q
```

**Expected Results:**
- Reddit: ~400 posts across 8 AI/ML subreddits
- GitHub: ~200 AI repositories
- CoinGecko: ~200 crypto projects
- Correlation analysis: Will show "Insufficient data" until day 2

**Important Note on Data Collection:**
The example insights shown in this README are based on only 3 days of data collection. For more comprehensive and meaningful analysis:
- **Run for at least 7-14 days** to see trend patterns
- **30+ days** provides robust correlation analysis
- **Longer periods** reveal seasonal patterns and deeper insights
- **More data** improves the accuracy of sentiment analysis and market correlations

## The ETL and Analytics DAGs

The project is composed of three distinct DAGs, each with a specific responsibility.

### 1. `social_media_analytics_pipeline` (The Main ETL)
- **Schedule:** Runs daily.
- **Purpose:** Extracts data from external APIs, performs basic transformations, and loads it into the PostgreSQL warehouse.
- **Workflow:**
    1.  **Extract:** Pulls the latest data from Reddit, GitHub, and CoinGecko in parallel.
    2.  **Transform:** Cleans the data (e.g., converts date formats) and calculates high-level metrics.
    3.  **Load:** Inserts the data into the `reddit_posts`, `github_repos`, and `crypto_data` tables. It uses a composite primary key of `(id, run_date)` to create a daily snapshot of the data.
    4.  **Quality Checks:** Runs a series of data quality checks to ensure the loaded data is valid.
    5.  **Cleanup:** Removes temporary files.

### 2. `advanced_correlation_analysis` (Data Science & Analytics)
- **Schedule:** Runs daily at 2 AM.
- **Purpose:** Performs in-depth statistical analysis on the data that has been collected in the warehouse.
- **Workflow:**
    1.  **Correlation Analysis:** Queries the last 30 days of data to calculate statistical correlations and trends between Reddit activity, GitHub development, and crypto project counts.
    2.  **Sentiment Analysis:** Analyzes the average sentiment of AI-related Reddit posts over the last 7 days to gauge the public mood.

### 3. `data_monitoring_pipeline` (Monitoring & Alerting)
- **Schedule:** Runs every 6 hours.
- **Purpose:** Monitors the health and freshness of the data in the warehouse.
- **Workflow:**
    1.  **Data Freshness Check:** Verifies that the data in the main tables is not stale (i.e., not older than 25 hours).
    2.  **Performance Monitoring:** Checks the `data_quality_metrics` table for an excessive number of failures in the last 24 hours.

## Troubleshooting

### Common Issues and Solutions

#### Container Startup Failures
```bash
# If containers fail to start
docker-compose down
docker-compose up --build -d

# If database issues persist (WARNING: This deletes all data)
docker-compose down -v
docker-compose up --build -d

# Check specific container logs
docker-compose logs postgres
docker-compose logs airflow-webserver
```

#### API Rate Limiting
- **Reddit**: Pipeline automatically handles 2-second delays between requests
- **GitHub**: Add `GITHUB_TOKEN` to `.env` file for higher rate limits (5000 requests/hour vs 60)
- **CoinGecko**: No authentication required, but has rate limits (~50 requests/minute)

#### DAG Execution Issues
```bash
# Check DAG import errors
docker-compose logs airflow-scheduler

# Restart specific services
docker-compose restart airflow-scheduler
docker-compose restart airflow-webserver

# Clear DAG cache
docker-compose exec airflow-webserver airflow db clean
```

#### Data Quality Issues
- **Missing data**: Check task logs for API errors or network issues
- **Insufficient correlation data**: Requires multiple days of data for meaningful analysis (minimum 7 days, optimal 30+ days)
- **Quality check failures**: Review `data_quality_metrics` table for specific issues
- **Limited insights**: Early days show basic patterns; deeper insights emerge after 2-4 weeks of continuous operation

#### Performance Issues
```bash
# Monitor resource usage
docker stats

# Check disk space
docker system df

# Clean up unused Docker resources
docker system prune -f
```

### Maintenance Tasks

#### Weekly Maintenance
```bash
# Clean up old Airflow logs
docker-compose exec airflow-webserver airflow db clean

# Update container images
docker-compose pull
docker-compose up --build -d

# Backup database
docker-compose exec postgres pg_dump -U airflow airflow > backup_$(date +%Y%m%d).sql
```

#### Scaling Options
- **Increase worker concurrency**: Modify `worker_concurrency` in `docker-compose.yml`
- **Add more workers**: Scale the `airflow-worker` service
- **Optimize PostgreSQL**: Tune database settings for larger datasets

### Data Persistence and Backup

#### Data Storage
- **Database data**: Persisted in Docker volume `postgres_db_volume`
- **Logs**: Stored in `./logs` directory on host
- **Temporary files**: Automatically cleaned up after each DAG run

#### Backup Strategy
```bash
# Create database backup
docker-compose exec postgres pg_dump -U airflow airflow > backup.sql

# Restore from backup
docker-compose exec -T postgres psql -U airflow airflow < backup.sql
```

### Security Considerations

#### Development vs Production
**Development (Current Setup):**
- Uses default `admin/admin` credentials
- Exposes all ports for debugging
- Includes sample data and test queries

**Production Recommendations:**
- Change default passwords immediately
- Use environment-specific secrets management
- Configure proper logging and monitoring
- Implement network security and access controls
- Use HTTPS for web interface

#### API Security
- Store API keys in environment variables only
- Use GitHub personal access tokens for higher rate limits
- Never commit secrets to version control
- Rotate API keys regularly

## Running Analytics Queries

You can connect to the PostgreSQL database directly to run your own queries and explore the data.

```bash
docker-compose exec postgres psql -U airflow -d airflow
```

## Example Insights

Here are the queries you ran and what the results tell us about the current state of the AI ecosystem.

### Query 1: Daily Ecosystem Health Check
This query provides a high-level summary of AI activity for the current day across all three platforms.

```sql
-- Ecosystem Health Check
SELECT 
    'Reddit AI Activity' as metric,
    COUNT(CASE WHEN ai_related THEN 1 END) as count,
    ROUND(AVG(CASE WHEN ai_related THEN sentiment_score END)::numeric, 3) as avg_value
FROM reddit_posts 
WHERE DATE(extracted_at) = CURRENT_DATE
UNION ALL
SELECT 
    'GitHub AI Repos' as metric,
    COUNT(CASE WHEN ai_related THEN 1 END) as count,
    ROUND(AVG(CASE WHEN ai_related THEN stars END)::numeric, 0) as avg_value
FROM github_repos 
WHERE DATE(extracted_at) = CURRENT_DATE
UNION ALL
SELECT 
    'Crypto AI Projects' as metric,
    COUNT(CASE WHEN ai_related THEN 1 END) as count,
    ROUND(AVG(CASE WHEN ai_related THEN current_price END)::numeric, 2) as avg_value
FROM crypto_data 
WHERE DATE(extracted_at) = CURRENT_DATE;
```
**Results:**
```
       metric       | count | avg_value 
--------------------+-------+-----------
 Reddit AI Activity |   246 |     0.089
 GitHub AI Repos    |   133 |    5436
 Crypto AI Projects |    15 |      3.04
```
**Conclusion:** On this day, there were 246 AI-related Reddit posts with a slightly positive average sentiment. 133 AI-related GitHub repos were identified, with a high average star count, indicating significant interest. The number of AI-focused crypto projects is much smaller.

**Note:** This analysis represents only 3 days of data collection. Extended operation will provide more comprehensive insights into trends and patterns.

### Query 2: Top Trending Reddit Posts
This query identifies the most popular (highest score) AI-related posts of the day.

```sql
-- Trending Topics on Reddit
SELECT 
    title,
    score,
    ROUND(sentiment_score::numeric, 3) as sentiment,
    subreddit
FROM reddit_posts 
WHERE ai_related = true 
  AND DATE(extracted_at) = CURRENT_DATE
ORDER BY score DESC 
LIMIT 10;
```
**Conclusion:** The most viral "AI" content originates from the general `r/technology` subreddit and focuses on mainstream news about policy and corporate announcements rather than deep technical discussions. This drives a high volume of engagement but a relatively neutral sentiment.

### Query 3: Hottest GitHub Repos
This query finds the most-starred AI repositories currently in the database.

```sql
-- Hottest GitHub AI Repos
SELECT 
    name,
    stars,
    language,
    LEFT(description, 60) as description_preview
FROM github_repos 
WHERE ai_related = true 
ORDER BY stars DESC 
LIMIT 5;
```
**Conclusion:** The pipeline is successfully identifying foundational, industry-standard AI libraries like `opencv` and `keras`. This provides a stable and reliable signal for overall developer interest and activity in the AI space.

## How to Use the Pipeline

### Daily Operations

#### Manual DAG Execution
- **Trigger a DAG:** In the Airflow UI, find the DAG you want to run, and click the "Play" button (▶) on the right
- **View Progress:** Click on the DAG name to see the Graph View with task status
- **Check Logs:** Click on a task square, then "Log" tab to view detailed execution logs
- **Monitor Quality:** Check the "Browse" menu for data quality metrics

#### Automated Scheduling
- **Main ETL**: Runs daily at midnight (configurable in DAG definition)
- **Analytics**: Runs daily at 2 AM after main ETL completes
- **Monitoring**: Runs every 6 hours to check data freshness

### Data Exploration

#### Database Access
```bash
# Connect to database
docker-compose exec postgres psql -U airflow -d airflow

# Quick data check
SELECT 
    'Reddit' as source, COUNT(*) as records 
FROM reddit_posts WHERE DATE(extracted_at) = CURRENT_DATE
UNION ALL
SELECT 
    'GitHub' as source, COUNT(*) as records 
FROM github_repos WHERE DATE(extracted_at) = CURRENT_DATE
UNION ALL
SELECT 
    'Crypto' as source, COUNT(*) as records 
FROM crypto_data WHERE DATE(extracted_at) = CURRENT_DATE;
```

#### Available Analytics
- **Trend Analysis**: Use `sql/analytics_queries.sql` for pre-built queries
- **Correlation Reports**: Check logs from `advanced_correlation_analysis` DAG
- **Data Quality**: Monitor `data_quality_metrics` table for pipeline health

### Adding More Data
- **Daily Snapshots**: Each run creates a new daily snapshot with `run_date`
- **Historical Analysis**: Data accumulates over time for trend analysis
- **Custom Queries**: Add your own analysis queries to explore patterns

### Data Collection Timeline for Insights
- **Days 1-3**: Basic data collection, limited correlation analysis
- **Week 1**: Initial trend identification, basic sentiment patterns
- **Week 2-4**: Meaningful correlation analysis, weekly trend patterns
- **Month 1+**: Robust statistical analysis, seasonal patterns, market cycle correlations
- **3+ Months**: Deep insights into AI ecosystem dynamics, predictive patterns

**Recommendation**: Let the pipeline run continuously for at least 30 days to unlock the full analytical potential and generate actionable business insights.

## Future Improvements

- **Secrets Management:** Move secrets from the `.env` file to a more secure backend like HashiCorp Vault or AWS Secrets Manager.
- **CI/CD:** Implement a GitHub Actions workflow to automatically run tests, lint code, and build the Docker image on pull requests.
- **Advanced Analytics:** Enhance the correlation analysis to account for time lags (e.g., does a spike in Reddit posts lead to a spike in GitHub commits a week later?).
- **Alerting:** Integrate with Slack or email to send notifications when the data monitoring pipeline detects an issue.
- **Visualization:** Add a tool like Superset or Metabase to create dashboards on top of the PostgreSQL data warehouse. 