# plugins/hooks/github_hook.py
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import requests
import time
import logging
from typing import List, Dict, Optional

class GitHubHook(BaseHook):
    """Custom hook for GitHub API with authentication and rate limiting"""
    
    def __init__(self, github_token: Optional[str] = None):
        self.github_token = github_token or Variable.get("GITHUB_TOKEN", default_var=None)
        self.base_url = "https://api.github.com"
        self.session = requests.Session()
        
        if self.github_token:
            self.session.headers.update({
                'Authorization': f'token {self.github_token}',
                'Accept': 'application/vnd.github.v3+json'
            })
        else:
            logging.warning("No GitHub token provided - using unauthenticated requests (lower rate limits)")
            self.session.headers.update({'Accept': 'application/vnd.github.v3+json'})
    
    def _check_rate_limit(self):
        """Check and handle GitHub API rate limits"""
        try:
            response = self.session.get(f"{self.base_url}/rate_limit")
            
            if response.status_code == 200:
                rate_info = response.json()
                remaining = rate_info['rate']['remaining']
                reset_time = rate_info['rate']['reset']
                
                if remaining < 10:  # Less than 10 requests remaining
                    current_time = time.time()
                    wait_time = reset_time - current_time + 60  # Add 1 minute buffer
                    
                    if wait_time > 0:
                        logging.warning(f"Rate limit nearly exceeded. Waiting {wait_time:.0f} seconds")
                        time.sleep(wait_time)
        except Exception as e:
            logging.warning(f"Could not check rate limit: {e}")
    
    def search_repositories(self, query: str, sort: str = "stars", 
                          per_page: int = 100, page: int = 2) -> List[Dict]:
        """Search for repositories on GitHub"""
        self._check_rate_limit()
        
        url = f"{self.base_url}/search/repositories"
        params = {
            'q': query,
            'sort': sort,
            'order': 'desc',
            'per_page': min(per_page, 100),  # GitHub max is 100
            'page': page
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            repositories = data['items']
            
            processed_repos = []
            for repo in repositories:
                processed_repos.append({
                    'id': repo['id'],
                    'name': repo['name'],
                    'full_name': repo['full_name'],
                    'description': repo.get('description', ''),
                    'stars': repo['stargazers_count'],
                    'forks': repo['forks_count'],
                    'watchers': repo['watchers_count'],
                    'language': repo.get('language', ''),
                    'created_at': repo['created_at'],
                    'updated_at': repo['updated_at'],
                    'size_kb': repo['size'],
                    'open_issues': repo['open_issues_count'],
                    'topics': repo.get('topics', [])
                })
            
            logging.info(f"Successfully extracted {len(processed_repos)} repositories")
            return processed_repos
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching GitHub data: {e}")
            raise