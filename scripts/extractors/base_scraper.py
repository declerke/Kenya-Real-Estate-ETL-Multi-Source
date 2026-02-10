import requests
from bs4 import BeautifulSoup
import time
import random
import logging
from typing import Optional, Dict
from config.settings import SCRAPING_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseScraper:
    def __init__(self, site_name: str):
        self.site_name = site_name
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': SCRAPING_CONFIG['user_agent'],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        try:
            self._polite_delay()
            logger.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=SCRAPING_CONFIG['timeout'])
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def _polite_delay(self):
        delay = random.uniform(SCRAPING_CONFIG['delay_min'], SCRAPING_CONFIG['delay_max'])
        time.sleep(delay)
    
    def extract_listings(self, max_pages: int = 5) -> list:
        raise NotImplementedError("Subclasses must implement extract_listings method")
    
    def parse_listing_page(self, soup: BeautifulSoup) -> list:
        raise NotImplementedError("Subclasses must implement parse_listing_page method")
    
    def extract_listing_details(self, listing_url: str) -> Optional[Dict]:
        raise NotImplementedError("Subclasses must implement extract_listing_details method")