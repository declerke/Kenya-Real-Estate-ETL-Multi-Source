from typing import Optional, Dict, List
from bs4 import BeautifulSoup
import logging
from scripts.extractors.base_scraper import BaseScraper
from config.settings import SITE_CONFIGS
from datetime import datetime

logger = logging.getLogger(__name__)

class HaoFinderScraper(BaseScraper):
    def __init__(self):
        super().__init__('haofinder')
        self.config = SITE_CONFIGS['haofinder']
        
    def extract_listings(self, max_pages: int = 5) -> List[Dict]:
        all_listings = []
        
        for page_num in range(1, max_pages + 1):
            # 2026 UPDATE: Using the active /properties endpoint
            page_url = f"{self.config['search_url']}?page={page_num}"
            soup = self.fetch_page(page_url)
            
            if not soup:
                logger.warning(f"Failed to fetch page {page_num}")
                continue
                
            listings = self.parse_listing_page(soup)
            logger.info(f"Extracted {len(listings)} listings from page {page_num}")
            all_listings.extend(listings)
            
            if len(listings) == 0:
                break
                
        return all_listings
    
    def parse_listing_page(self, soup: BeautifulSoup) -> List[Dict]:
        listings = []
        # 2026 UPDATE: HaoFinder now uses wrapper classes like 'property-item' or 'listing-wrapper'
        listing_cards = soup.select('.property-item') or soup.select('.listing-wrapper')
        
        if not listing_cards:
            listing_cards = soup.find_all('div', class_='property-listing') or soup.find_all('div', class_='listing-card')
        
        for card in listing_cards:
            try:
                listing_data = self._extract_card_data(card)
                if listing_data and listing_data.get('listing_url'):
                    listings.append(listing_data)
            except Exception as e:
                logger.error(f"Error parsing listing card: {e}")
                continue
                
        return listings
    
    def _extract_card_data(self, card) -> Optional[Dict]:
        try:
            # Flexible selectors for modern class names
            title_elem = card.select_one('.property-title, .listing-title, h3')
            title = title_elem.get_text(strip=True) if title_elem else 'N/A'
            
            link_elem = card.find('a', href=True)
            if link_elem and link_elem.get('href'):
                listing_url = link_elem['href']
                if not listing_url.startswith('http'):
                    listing_url = self.config['base_url'].rstrip('/') + '/' + listing_url.lstrip('/')
            else:
                return None
            
            price_elem = card.select_one('.property-price, .price, .amount')
            price_raw = price_elem.get_text(strip=True) if price_elem else 'N/A'
            
            location_elem = card.select_one('.property-location, .location-info, .address')
            location_raw = location_elem.get_text(strip=True) if location_elem else 'N/A'
            
            description_elem = card.select_one('.property-description, .excerpt, p')
            description = description_elem.get_text(strip=True) if description_elem else 'N/A'
            
            # Feature extraction for HaoFinder
            features = card.select('.property-feature, .amenity, span')
            bedrooms_raw = 'N/A'
            bathrooms_raw = 'N/A'
            area_raw = 'N/A'
            
            for feature in features:
                text = feature.get_text(strip=True).lower()
                if 'bed' in text:
                    bedrooms_raw = text
                elif 'bath' in text:
                    bathrooms_raw = text
                elif 'sqm' in text or 'sq' in text or 'm²' in text:
                    area_raw = text
            
            property_type_elem = card.select_one('.property-type, .type-label')
            property_type_raw = property_type_elem.get_text(strip=True) if property_type_elem else 'N/A'
            
            return {
                'source_site': self.site_name,
                'listing_url': listing_url,
                'title': title,
                'description': description,
                'price_raw': price_raw,
                'location_raw': location_raw,
                'bedrooms_raw': bedrooms_raw,
                'bathrooms_raw': bathrooms_raw,
                'area_raw': area_raw,
                'property_type_raw': property_type_raw,
                'scraped_at': datetime.utcnow()
            }
        except Exception as e:
            logger.error(f"Error extracting card data: {e}")
            return None
    
    def extract_listing_details(self, listing_url: str) -> Optional[Dict]:
        soup = self.fetch_page(listing_url)
        if not soup:
            return None
            
        try:
            details = {}
            title_elem = soup.select_one('.property-title, h1')
            details['title'] = title_elem.get_text(strip=True) if title_elem else 'N/A'
            
            price_elem = soup.select_one('.property-price, .price-value')
            details['price_raw'] = price_elem.get_text(strip=True) if price_elem else 'N/A'
            
            desc_elem = soup.select_one('.property-description, #description')
            details['description'] = desc_elem.get_text(strip=True) if desc_elem else 'N/A'
            
            return details
        except Exception as e:
            logger.error(f"Error extracting listing details: {e}")
            return None
