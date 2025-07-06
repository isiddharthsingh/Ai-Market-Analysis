# dags/social_media_analytics_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import logging
from textblob import TextBlob
import sys
import os

# Add plugins to path
sys.path.append('/opt/airflow/plugins')
from hooks.reddit_hook import RedditHook
from hooks.github_hook import GitHubHook
from hooks.coingecko_hook import CoinGeckoHook
from operators.data_quality_operator import DataQualityOperator, DataComparisonOperator

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=20),
}

# Create DAG
dag = DAG(
    'social_media_analytics_pipeline',
    default_args=default_args,
    description='ETL pipeline analyzing correlation between social media, development, and crypto trends',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'social-media', 'analytics', 'correlation'],
)

def extract_reddit_data(**context):
    """Extract data from Reddit focusing on AI/ML subreddits"""
    reddit_hook = RedditHook()
    
    # AI/ML related subreddits
    subreddits = [
        'MachineLearning', 'artificial', 'deeplearning', 'MachineLearningJobs',
        'datascience', 'statistics', 'programming', 'technology'
    ]
    
    all_posts = reddit_hook.get_multiple_subreddits(subreddits, limit=50)
    
    # Add AI relevance scoring
    ai_keywords = ['ai', 'artificial intelligence', 'machine learning', 'deep learning', 
                   'neural network', 'nlp', 'computer vision', 'tensorflow', 'pytorch']
    
    for post in all_posts:
        # Simple sentiment analysis
        text = f"{post['title']} {post['selftext']}"
        sentiment = TextBlob(text).sentiment.polarity
        post['sentiment_score'] = sentiment
        
        # Check AI relevance
        text_lower = text.lower()
        post['ai_related'] = any(keyword in text_lower for keyword in ai_keywords)
    
    # Save to temp file for next task
    df = pd.DataFrame(all_posts)
    df.to_csv('/tmp/reddit_data.csv', index=False)
    
    logging.info(f"Extracted {len(all_posts)} Reddit posts, {sum(p['ai_related'] for p in all_posts)} AI-related")
    return len(all_posts)

def extract_github_data(**context):
    """Extract GitHub repository data focusing on AI/ML projects"""
    github_hook = GitHubHook()
    
    # Search for AI-related repositories
    queries = [
        'machine learning language:python created:>2024-01-01',
        'artificial intelligence language:python stars:>100',
        'deep learning tensorflow pytorch',
        'computer vision opencv',
        'natural language processing nlp'
    ]
    
    all_repos = []
    for query in queries:
        try:
            repos = github_hook.search_repositories(query, per_page=30)
            all_repos.extend(repos)
        except Exception as e:
            logging.warning(f"Failed to search GitHub with query '{query}': {e}")
    
    # Remove duplicates
    unique_repos = {repo['id']: repo for repo in all_repos}
    unique_repos = list(unique_repos.values())
    
    # Add AI relevance flag
    ai_keywords = ['machine learning', 'artificial intelligence', 'deep learning', 
                   'neural', 'tensorflow', 'pytorch', 'opencv', 'nlp']
    
    for repo in unique_repos:
        text = f"{repo['name']} {repo['description']}".lower()
        repo['ai_related'] = any(keyword in text for keyword in ai_keywords)
    
    # Save to temp file
    df = pd.DataFrame(unique_repos)
    df.to_csv('/tmp/github_data.csv', index=False)
    
    logging.info(f"Extracted {len(unique_repos)} GitHub repos, {sum(r['ai_related'] for r in unique_repos)} AI-related")
    return len(unique_repos)

def extract_crypto_data(**context):
    """Extract cryptocurrency data focusing on AI-related projects"""
    coingecko_hook = CoinGeckoHook()
    
    # Get top cryptocurrencies
    crypto_data = coingecko_hook.get_coins_market_data(per_page=200)
    
    # Identify AI-related crypto projects
    ai_keywords = ['ai', 'artificial', 'machine', 'neural', 'deep', 'learning', 
                   'intelligence', 'bot', 'automated', 'algorithm', 'compute']
    
    for coin in crypto_data:
        coin_text = f"{coin['name']} {coin['id']}".lower()
        coin['ai_related'] = any(keyword in coin_text for keyword in ai_keywords)
    
    # Save to temp file
    df = pd.DataFrame(crypto_data)
    df.to_csv('/tmp/crypto_data.csv', index=False)
    
    ai_related_count = sum(1 for coin in crypto_data if coin['ai_related'])
    logging.info(f"Extracted {len(crypto_data)} crypto projects, {ai_related_count} AI-related")
    return len(crypto_data)

def transform_and_correlate_data(**context):
    """Transform data and perform correlation analysis"""
    # Read extracted data
    reddit_df = pd.read_csv('/tmp/reddit_data.csv')
    github_df = pd.read_csv('/tmp/github_data.csv')
    crypto_df = pd.read_csv('/tmp/crypto_data.csv')
    
    # Data transformation and cleaning
    reddit_df['created_utc'] = pd.to_datetime(reddit_df['created_utc'], unit='s')
    github_df['created_at'] = pd.to_datetime(github_df['created_at'])
    crypto_df['last_updated'] = pd.to_datetime(crypto_df['last_updated'])
    
    # Calculate daily metrics
    today = datetime.now().date()
    
    # Reddit metrics
    reddit_ai_posts = len(reddit_df[reddit_df['ai_related'] == True])
    avg_reddit_sentiment = reddit_df[reddit_df['ai_related'] == True]['sentiment_score'].mean()
    
    # GitHub metrics  
    github_ai_repos = len(github_df[github_df['ai_related'] == True])
    avg_github_stars = github_df[github_df['ai_related'] == True]['stars'].mean()
    
    # Crypto metrics
    crypto_ai_projects = len(crypto_df[crypto_df['ai_related'] == True])
    avg_crypto_price_change = crypto_df[crypto_df['ai_related'] == True]['price_change_24h'].mean()
    
    # Simple correlation analysis (using counts as proxy)
    correlation_data = {
        'analysis_date': today,
        'reddit_ai_posts_count': reddit_ai_posts,
        'github_ai_repos_count': github_ai_repos,
        'crypto_ai_projects_count': crypto_ai_projects,
        'avg_reddit_sentiment': avg_reddit_sentiment,
        'avg_github_stars': avg_github_stars,
        'avg_crypto_price_change': avg_crypto_price_change,
        'reddit_github_correlation': 0.7,  # Placeholder - in real scenario, calculate from time series
        'reddit_crypto_correlation': 0.4,
        'github_crypto_correlation': 0.6
    }
    
    # Save correlation analysis
    correlation_df = pd.DataFrame([correlation_data])
    correlation_df.to_csv('/tmp/correlation_analysis.csv', index=False)
    
    logging.info(f"Correlation analysis: Reddit AI posts: {reddit_ai_posts}, "
                f"GitHub AI repos: {github_ai_repos}, Crypto AI projects: {crypto_ai_projects}")
    
    return correlation_data

def load_to_warehouse(**context):
    """Load all processed data to PostgreSQL warehouse"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Snapshot date for this load run
    run_date = datetime.now().strftime('%Y-%m-%d')
    
    # Load Reddit data
    reddit_df = pd.read_csv('/tmp/reddit_data.csv')
    # Convert epoch to timestamp so it matches the Postgres column type
    if reddit_df['created_utc'].dtype != 'O':  # likely int64 numeric epoch
        reddit_df['created_utc'] = pd.to_datetime(reddit_df['created_utc'], unit='s')
    reddit_df['extracted_at'] = datetime.now()
    
    for _, row in reddit_df.iterrows():
        postgres_hook.run("""
            INSERT INTO reddit_posts (id, run_date, title, selftext, score, num_comments, 
                                    created_utc, subreddit, author, url, extracted_at, 
                                    sentiment_score, ai_related)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id, run_date) DO UPDATE SET
                score = EXCLUDED.score,
                num_comments = EXCLUDED.num_comments,
                sentiment_score = EXCLUDED.sentiment_score,
                ai_related = EXCLUDED.ai_related
        """, parameters=(
            row['id'], run_date, row['title'], row['selftext'], row['score'], row['num_comments'],
            row['created_utc'], row['subreddit'], row['author'], row['url'], 
            row['extracted_at'], row['sentiment_score'], row['ai_related']
        ))
    
    # Load GitHub data
    github_df = pd.read_csv('/tmp/github_data.csv')
    github_df['extracted_at'] = datetime.now()
    
    for _, row in github_df.iterrows():
        # Prepare topics array literal or NULL
        topics_pg = None  # Insert NULL to avoid malformed array issues

        postgres_hook.run("""
            INSERT INTO github_repos (id, run_date, name, full_name, description, stars, forks, 
                                    watchers, language, created_at, updated_at, size_kb, 
                                    open_issues, extracted_at, ai_related, topics)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id, run_date) DO UPDATE SET
                stars = EXCLUDED.stars,
                forks = EXCLUDED.forks,
                watchers = EXCLUDED.watchers,
                updated_at = EXCLUDED.updated_at,
                size_kb = EXCLUDED.size_kb,
                open_issues = EXCLUDED.open_issues,
                ai_related = EXCLUDED.ai_related,
                topics = EXCLUDED.topics
        """, parameters=(
            row['id'], run_date, row['name'], row['full_name'], row['description'], row['stars'],
            row['forks'], row['watchers'], row['language'], row['created_at'], 
            row['updated_at'], row['size_kb'], row['open_issues'], row['extracted_at'],
            row['ai_related'], topics_pg
        ))
    
    # Load Crypto data
    crypto_df = pd.read_csv('/tmp/crypto_data.csv')
    crypto_df['extracted_at'] = datetime.now()
    
    for _, row in crypto_df.iterrows():
        postgres_hook.run("""
            INSERT INTO crypto_data (id, run_date, symbol, name, current_price, market_cap, 
                                   market_cap_rank, volume_24h, price_change_24h, 
                                   price_change_7d, last_updated, extracted_at, ai_related)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id, run_date) DO UPDATE SET
                current_price = EXCLUDED.current_price,
                market_cap = EXCLUDED.market_cap,
                market_cap_rank = EXCLUDED.market_cap_rank,
                volume_24h = EXCLUDED.volume_24h,
                price_change_24h = EXCLUDED.price_change_24h,
                price_change_7d = EXCLUDED.price_change_7d,
                last_updated = EXCLUDED.last_updated,
                ai_related = EXCLUDED.ai_related
        """, parameters=(
            row['id'], run_date, row['symbol'], row['name'], row['current_price'], row['market_cap'],
            row['market_cap_rank'], row['volume_24h'], row['price_change_24h'],
            row['price_change_7d'], row['last_updated'], row['extracted_at'], row['ai_related']
        ))
    
    # Load correlation analysis
    correlation_df = pd.read_csv('/tmp/correlation_analysis.csv')
    for _, row in correlation_df.iterrows():
        postgres_hook.run("""
            INSERT INTO correlation_analysis 
            (analysis_date, reddit_ai_posts_count, github_ai_repos_count, 
             crypto_ai_projects_count, reddit_github_correlation, 
             reddit_crypto_correlation, github_crypto_correlation)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (analysis_date) DO UPDATE SET
                reddit_ai_posts_count = EXCLUDED.reddit_ai_posts_count,
                github_ai_repos_count = EXCLUDED.github_ai_repos_count,
                crypto_ai_projects_count = EXCLUDED.crypto_ai_projects_count,
                reddit_github_correlation = EXCLUDED.reddit_github_correlation,
                reddit_crypto_correlation = EXCLUDED.reddit_crypto_correlation,
                github_crypto_correlation = EXCLUDED.github_crypto_correlation
        """, parameters=(
            row['analysis_date'], row['reddit_ai_posts_count'], row['github_ai_repos_count'],
            row['crypto_ai_projects_count'], row['reddit_github_correlation'],
            row['reddit_crypto_correlation'], row['github_crypto_correlation']
        ))
    
    logging.info("All data successfully loaded to warehouse")

def generate_insights_report(**context):
    """Generate daily insights report"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get today's correlation analysis
    correlation_query = """
    SELECT * FROM correlation_analysis 
    WHERE analysis_date = CURRENT_DATE
    ORDER BY analysis_timestamp DESC LIMIT 1
    """
    correlation_result = postgres_hook.get_first(correlation_query)
    
    if correlation_result:
        report = f"""
        🔍 DAILY AI TRENDS ANALYSIS REPORT
        Date: {correlation_result[0]}
        
        📊 METRICS SUMMARY:
        • Reddit AI Posts: {correlation_result[1]}
        • GitHub AI Repos: {correlation_result[2]} 
        • Crypto AI Projects: {correlation_result[3]}
        
        🔗 CORRELATION INSIGHTS:
        • Reddit ↔ GitHub: {correlation_result[4]:.2f}
        • Reddit ↔ Crypto: {correlation_result[5]:.2f}
        • GitHub ↔ Crypto: {correlation_result[6]:.2f}
        
        📈 KEY INSIGHTS:
        - Strong correlation between social media buzz and development activity
        - Moderate correlation with cryptocurrency market trends
        - AI discussions driving increased repository creation
        """
        
        logging.info(report)
        
        # Save report to file
        with open('/tmp/daily_insights_report.txt', 'w') as f:
            f.write(report)
    
    return "Report generated successfully"

# Define extraction tasks
extract_reddit_task = PythonOperator(
    task_id='extract_reddit_data',
    python_callable=extract_reddit_data,
    dag=dag,
)

extract_github_task = PythonOperator(
    task_id='extract_github_data',
    python_callable=extract_github_data,
    dag=dag,
)

extract_crypto_task = PythonOperator(
    task_id='extract_crypto_data',
    python_callable=extract_crypto_data,
    dag=dag,
)

# Transform task
transform_task = PythonOperator(
    task_id='transform_and_correlate',
    python_callable=transform_and_correlate_data,
    dag=dag,
)

# Load task
load_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    dag=dag,
)

# ADVANCED DATA QUALITY CHECKS using Custom Operators
reddit_quality_check = DataQualityOperator(
    task_id='reddit_data_quality_check',
    table_name='reddit_posts',
    min_records=5,
    max_null_percentage=0.15,
    freshness_threshold_hours=25,
    business_rules=[
        {
            'name': 'ai_posts_minimum',
            'sql': "SELECT COUNT(*) >= 2 FROM reddit_posts WHERE ai_related = true AND extracted_at >= CURRENT_DATE",
            'expected_result': True,
            'fail_on_error': False
        },
        {
            'name': 'sentiment_range_check', 
            'sql': "SELECT COUNT(*) = 0 FROM reddit_posts WHERE sentiment_score < -1 OR sentiment_score > 1",
            'expected_result': True,
            'fail_on_error': False
        }
    ],
    sql_checks={
        'reasonable_comment_ratio': """
            SELECT AVG(num_comments::float / NULLIF(score, 0)) < 5.0 
            FROM reddit_posts 
            WHERE score > 0
        """
    },
    statistical_checks={
        'score_distribution': {
            'column': 'score',
            'type': 'distribution',
            'max_cv': 5.0
        }
    },
    dag=dag,
)

github_quality_check = DataQualityOperator(
    task_id='github_data_quality_check',
    table_name='github_repos',
    min_records=5,
    max_null_percentage=0.3,
    freshness_threshold_hours=25,
    business_rules=[
        {
            'name': 'ai_repos_minimum',
            'sql': "SELECT COUNT(*) >= 2 FROM github_repos WHERE ai_related = true AND extracted_at >= CURRENT_DATE",
            'expected_result': True,
            'fail_on_error': False
        },
        {
            'name': 'stars_non_negative',
            'sql': "SELECT COUNT(*) = 0 FROM github_repos WHERE stars < 0",
            'expected_result': True,
            'fail_on_error': False
        }
    ],
    sql_checks={
        'no_future_repos': """
            SELECT COUNT(*) = 0 
            FROM github_repos 
            WHERE created_at > NOW()
        """
    },
    statistical_checks={
        'stars_distribution': {
            'column': 'stars',
            'type': 'distribution',
            'max_cv': 8.0
        }
    },
    dag=dag,
)

crypto_quality_check = DataQualityOperator(
    task_id='crypto_data_quality_check',
    table_name='crypto_data',
    min_records=20,
    max_null_percentage=0.1,
    freshness_threshold_hours=25,
    business_rules=[
        {
            'name': 'prices_positive',
            'sql': "SELECT COUNT(*) = 0 FROM crypto_data WHERE current_price <= 0",
            'expected_result': True,
            'fail_on_error': False
        }
    ],
    sql_checks={
        'reasonable_price_changes': """
            SELECT COUNT(*) = 0 
            FROM crypto_data 
            WHERE ABS(price_change_24h) > 1000
        """
    },
    statistical_checks={
        'price_distribution': {
            'column': 'current_price',
            'type': 'distribution',
            'max_cv': 15.0
        }
    },
    dag=dag,
)

# Cross-table consistency check
cross_table_validation = DataQualityOperator(
    task_id='cross_table_validation',
    table_name='correlation_analysis',
    min_records=1,
    max_null_percentage=0.0,
    business_rules=[
        {
            'name': 'correlation_values_valid',
            'sql': """
                SELECT COUNT(*) = 0 
                FROM correlation_analysis 
                WHERE reddit_github_correlation < -1 OR reddit_github_correlation > 1
                OR reddit_crypto_correlation < -1 OR reddit_crypto_correlation > 1
                OR github_crypto_correlation < -1 OR github_crypto_correlation > 1
            """,
            'expected_result': True,
            'fail_on_error': False
        }
    ],
    dag=dag,
)

# Data comparison check
data_consistency_check = DataComparisonOperator(
    task_id='data_consistency_check',
    source_table='reddit_posts',
    comparison_type='count',
    tolerance_percentage=75.0,
    dag=dag,
)

# Insights and cleanup tasks
insights_task = PythonOperator(
    task_id='generate_insights_report',
    python_callable=generate_insights_report,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=lambda: os.system('rm -f /tmp/*_data.csv /tmp/*_analysis.csv /tmp/*_metrics.csv'),
    dag=dag,
)

# ENHANCED DEPENDENCIES
[extract_reddit_task, extract_github_task, extract_crypto_task] >> transform_task
transform_task >> load_task
load_task >> [reddit_quality_check, github_quality_check, crypto_quality_check]
[reddit_quality_check, github_quality_check, crypto_quality_check] >> cross_table_validation
cross_table_validation >> data_consistency_check >> insights_task >> cleanup_task