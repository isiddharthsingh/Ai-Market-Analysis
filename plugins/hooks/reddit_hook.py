from airflow.hooks.base import BaseHook
import requests
import time
import logging
from typing import List, Dict

class RedditHook(BaseHook):
    """Custom hook for Reddit API with rate limiting and error handling"""
    
    def __init__(self, user_agent: str = "DataPipeline/1.0"):
        self.user_agent = user_agent
        self.base_url = "https://www.reddit.com"
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})
        self.last_request_time = 0
        self.min_request_interval = 2  # Reddit rate limit: 1 request per 2 seconds
        
    def _rate_limit(self):
        """Implement rate limiting for Reddit API"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            logging.info(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def get_subreddit_posts(self, subreddit: str, sort_type: str = "hot", 
                           limit: int = 100) -> List[Dict]:
        """Get posts from a specific subreddit"""
        self._rate_limit()
        
        url = f"{self.base_url}/r/{subreddit}/{sort_type}.json"
        params = {'limit': limit}
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            posts = data['data']['children']
            
            processed_posts = []
            for post in posts:
                post_data = post['data']
                processed_posts.append({
                    'id': post_data['id'],
                    'title': post_data['title'],
                    'selftext': post_data.get('selftext', ''),
                    'score': post_data['score'],
                    'num_comments': post_data['num_comments'],
                    'created_utc': post_data['created_utc'],
                    'subreddit': post_data['subreddit'],
                    'author': post_data.get('author', '[deleted]'),
                    'url': post_data.get('url', '')
                })
            
            logging.info(f"Successfully extracted {len(processed_posts)} posts from r/{subreddit}")
            return processed_posts
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching Reddit data: {e}")
            raise
    
    def get_multiple_subreddits(self, subreddits: List[str], **kwargs) -> List[Dict]:
        """Get posts from multiple subreddits"""
        all_posts = []
        
        for subreddit in subreddits:
            try:
                posts = self.get_subreddit_posts(subreddit, **kwargs)
                all_posts.extend(posts)
            except Exception as e:
                logging.warning(f"Failed to get posts from r/{subreddit}: {e}")
                continue
        
        return all_posts