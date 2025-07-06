# plugins/hooks/coingecko_hook.py
from airflow.hooks.base import BaseHook
import requests
import logging
from typing import List, Dict

class CoinGeckoHook(BaseHook):
    """Custom hook for CoinGecko API"""
    
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
    
    def get_coins_market_data(self, vs_currency: str = "usd", per_page: int = 100, 
                             page: int = 1) -> List[Dict]:
        """Get market data for cryptocurrencies"""
        url = f"{self.base_url}/coins/markets"
        params = {
            'vs_currency': vs_currency,
            'order': 'market_cap_desc',
            'per_page': per_page,
            'page': page,
            'sparkline': False,
            'price_change_percentage': '24h,7d'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            coins = response.json()
            
            processed_coins = []
            for coin in coins:
                processed_coins.append({
                    'id': coin['id'],
                    'symbol': coin['symbol'],
                    'name': coin['name'],
                    'current_price': coin['current_price'],
                    'market_cap': coin['market_cap'],
                    'market_cap_rank': coin['market_cap_rank'],
                    'volume_24h': coin['total_volume'],
                    'price_change_24h': coin.get('price_change_percentage_24h'),
                    'price_change_7d': coin.get('price_change_percentage_7d_in_currency'),
                    'last_updated': coin['last_updated']
                })
            
            logging.info(f"Successfully extracted {len(processed_coins)} cryptocurrency records")
            return processed_coins
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching CoinGecko data: {e}")
            raise