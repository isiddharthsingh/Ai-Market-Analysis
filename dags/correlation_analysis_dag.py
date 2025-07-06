# dags/correlation_analysis_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import numpy as np
import logging

default_args = {
    'owner': 'data-science-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'advanced_correlation_analysis',
    default_args=default_args,
    description='Advanced correlation and trend analysis',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['analytics', 'correlation', 'insights'],
)

def advanced_correlation_analysis(**context):
    """Perform advanced statistical correlation analysis"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get historical data for correlation analysis
    query = """
    SELECT 
        analysis_date,
        reddit_ai_posts_count,
        github_ai_repos_count, 
        crypto_ai_projects_count
    FROM correlation_analysis 
    WHERE analysis_date >= CURRENT_DATE - INTERVAL '30 days'
    ORDER BY analysis_date
    """
    
    # Workaround for '__extra__' parameter issue in SQLAlchemy URI
    # Use raw psycopg2 connection instead of the SQLAlchemy engine
    try:
        df = pd.read_sql(query, postgres_hook.get_conn())
    except Exception:
    df = pd.read_sql(query, postgres_hook.get_sqlalchemy_engine())
    
    if len(df) < 3:  # Need at least 3 days of data
        logging.warning("Insufficient data for correlation analysis")
        return "Insufficient data"
    
    # Calculate correlations
    reddit_data = df['reddit_ai_posts_count'].values
    github_data = df['github_ai_repos_count'].values  
    crypto_data = df['crypto_ai_projects_count'].values
    
    # Simple correlation calculation
    reddit_github_corr = np.corrcoef(reddit_data, github_data)[0, 1] if len(reddit_data) > 1 else 0
    reddit_crypto_corr = np.corrcoef(reddit_data, crypto_data)[0, 1] if len(reddit_data) > 1 else 0
    github_crypto_corr = np.corrcoef(github_data, crypto_data)[0, 1] if len(github_data) > 1 else 0
    
    # Time series analysis - detect trends
    if len(reddit_data) > 1:
        reddit_trend = np.polyfit(range(len(reddit_data)), reddit_data, 1)[0]
        github_trend = np.polyfit(range(len(github_data)), github_data, 1)[0]
        crypto_trend = np.polyfit(range(len(crypto_data)), crypto_data, 1)[0]
    else:
        reddit_trend = github_trend = crypto_trend = 0
    
    # Generate insights report
    insights = f"""
    🔬 ADVANCED CORRELATION ANALYSIS
    
    📊 Statistical Correlations ({len(df)}-day):
    • Reddit ↔ GitHub: {reddit_github_corr:.3f}
    • Reddit ↔ Crypto: {reddit_crypto_corr:.3f}  
    • GitHub ↔ Crypto: {github_crypto_corr:.3f}
    
    📈 Trend Analysis:
    • Reddit: {'↗️ Increasing' if reddit_trend > 0 else '↘️ Decreasing'} ({reddit_trend:+.2f}/day)
    • GitHub: {'↗️ Increasing' if github_trend > 0 else '↘️ Decreasing'} ({github_trend:+.2f}/day)
    • Crypto: {'↗️ Increasing' if crypto_trend > 0 else '↘️ Decreasing'} ({crypto_trend:+.2f}/day)
    
    🎯 Key Insights:
    {generate_insights(reddit_github_corr, reddit_crypto_corr, github_crypto_corr)}
    
    Data Quality: {len(df)} days analyzed
    """
    
    logging.info(insights)
    
    # Save analysis results
    with open('/tmp/advanced_correlation_analysis.txt', 'w') as f:
        f.write(insights)
    
    return {"reddit_github_corr": reddit_github_corr, "data_points": len(df)}

def generate_insights(reddit_github_corr, reddit_crypto_corr, github_crypto_corr):
    """Generate business insights from correlation data"""
    insights = []
    
    if reddit_github_corr > 0.5:
        insights.append("• Strong positive correlation: Reddit discussions drive GitHub activity")
    elif reddit_github_corr < -0.5:
        insights.append("• Strong negative correlation: Reddit discussions inversely related to GitHub activity")
    
    if reddit_crypto_corr > 0.3:
        insights.append("• Moderate correlation: Social buzz impacts crypto AI projects")
    
    if github_crypto_corr > 0.4:
        insights.append("• Moderate dev-market correlation: Development activity influences crypto valuations")
    
    if not insights:
        insights.append("• Correlations are weak - factors may be largely independent")
    
    return "\n    ".join(insights)

def sentiment_impact_analysis(**context):
    """Analyze how sentiment affects correlations"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get sentiment data from Reddit
    sentiment_query = """
    SELECT 
        DATE(extracted_at) as date,
        AVG(sentiment_score) as avg_sentiment,
        COUNT(*) as post_count,
        COUNT(CASE WHEN ai_related THEN 1 END) as ai_post_count
    FROM reddit_posts 
    WHERE extracted_at >= CURRENT_DATE - INTERVAL '7 days'
    AND sentiment_score IS NOT NULL
    GROUP BY DATE(extracted_at)
    ORDER BY date
    """
    
    sentiment_df = pd.read_sql(sentiment_query, postgres_hook.get_conn())
    
    if len(sentiment_df) > 1:
        # Analyze sentiment trends
        avg_sentiment = sentiment_df['avg_sentiment'].mean()
        sentiment_trend = np.polyfit(range(len(sentiment_df)), sentiment_df['avg_sentiment'], 1)[0]
        
        sentiment_analysis = f"""
        😊 SENTIMENT IMPACT ANALYSIS
        
        📊 7-Day Sentiment Metrics:
        • Average Sentiment: {avg_sentiment:.3f} (-1 to +1 scale)
        • Sentiment Trend: {'📈 Improving' if sentiment_trend > 0 else '📉 Declining'} ({sentiment_trend:+.4f}/day)
        • Total AI Posts: {sentiment_df['ai_post_count'].sum()}
        • Avg Posts/Day: {sentiment_df['post_count'].mean():.1f}
        
        🎯 Sentiment Insights:
        {generate_sentiment_insights(avg_sentiment, sentiment_trend)}
        """
        
        logging.info(sentiment_analysis)
        
        with open('/tmp/sentiment_analysis.txt', 'w') as f:
            f.write(sentiment_analysis)
    
    return "Sentiment analysis completed"

def generate_sentiment_insights(avg_sentiment, trend):
    """Generate insights from sentiment analysis"""
    insights = []
    
    if avg_sentiment > 0.1:
        insights.append("• Overall positive sentiment towards AI discussions")
    elif avg_sentiment < -0.1:
        insights.append("• Overall negative sentiment towards AI discussions")
    else:
        insights.append("• Neutral sentiment towards AI discussions")
    
    if trend > 0.01:
        insights.append("• Sentiment is improving over time")
    elif trend < -0.01:
        insights.append("• Sentiment is declining over time")
    
    if avg_sentiment > 0.2 and trend > 0:
        insights.append("• Strong positive momentum - may drive increased development activity")
    
    return "\n        ".join(insights)

# Define tasks
correlation_task = PythonOperator(
    task_id='advanced_correlation_analysis',
    python_callable=advanced_correlation_analysis,
    dag=dag,
)

sentiment_task = PythonOperator(
    task_id='sentiment_impact_analysis', 
    python_callable=sentiment_impact_analysis,
    dag=dag,
)

# Tasks can run in parallel
[correlation_task, sentiment_task]