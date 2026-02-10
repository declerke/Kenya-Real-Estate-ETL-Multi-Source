from typing import Optional, Dict, List
from bs4 import BeautifulSoup
import logging
from scripts.extractors.base_scraper import BaseScraper
from config.settings import SITE_CONFIGS
from datetime import datetime

logger = logging.getLogger(__name__)

class Property24Scraper(BaseScraper):
    def __init__(self):
        super().__init__('property24')
        self.config = SITE_CONFIGS['property24']
        # Site often requires a direct base_url for relative link joining
        self.base_url = "https://www.property24.co.ke"
        
    def extract_listings(self, max_pages: int = 5) -> List[Dict]:
        all_listings = []
        
        for page_num in range(1, max_pages + 1):
            # 2026 UPDATE: Property24 uses ?Page= with a capital 'P'
            page_url = f"{self.config['search_url']}?Page={page_num}"
            soup = self.fetch_page(page_url)
            
            if not soup:
                logger.warning(f"Failed to fetch page {page_num}")
                continue
                
            listings = self.parse_listing_page(soup)
            logger.info(f"Extracted {len(listings)} listings from page {page_num}")
            all_listings.extend(listings)
            
            # Stop if no listings found on the page
            if len(listings) == 0:
                break
                
        return all_listings
    
    def parse_listing_page(self, soup: BeautifulSoup) -> List[Dict]:
        listings = []
        # 2026 UPDATE: Selectors shifted to hyphenated classes or wrapper divs
        listing_cards = soup.select('div.p24-listing') or soup.select('div.p24_regularResult')
        
        if not listing_cards:
            listing_cards = soup.find_all('div', {'data-listing-id': True})
        
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
            # Flexible selector for title and links
            title_elem = card.select_one('.p24-title, .p24_title, h3')
            title = title_elem.get_text(strip=True) if title_elem else 'N/A'
            
            link_elem = card.find('a', href=True)
            if link_elem:
                listing_url = link_elem['href']
                if not listing_url.startswith('http'):
                    listing_url = self.base_url.rstrip('/') + '/' + listing_url.lstrip('/')
            else:
                return None
            
            price_elem = card.select_one('.p24-price, .p24_price')
            price_raw = price_elem.get_text(strip=True) if price_elem else 'N/A'
            
            location_elem = card.select_one('.p24-location, .p24_location')
            location_raw = location_elem.get_text(strip=True) if location_elem else 'N/A'
            
            description_elem = card.select_one('.p24-excerpt, .p24_excerpt')
            description = description_elem.get_text(strip=True) if description_elem else 'N/A'
            
            # Extract features (Beds, Baths, Size)
            bedrooms_raw = 'N/A'
            bathrooms_raw = 'N/A'
            area_raw = 'N/A'
            
            # Feature containers usually use icons followed by text
            feature_icons = card.select('.p24-icon, .p24_featureDetails i')
            for icon in feature_icons:
                parent = icon.parent
                val = parent.get_text(strip=True)
                icon_cls = "".join(icon.get('class', [])).lower()
                
                if 'bed' in icon_cls:
                    bedrooms_raw = val
                elif 'bath' in icon_cls:
                    bathrooms_raw = val
                elif 'size' in icon_cls or 'area' in icon_cls:
                    area_raw = val
            
            property_type_elem = card.select_one('.p24-propertyType, .p24_propertyType')
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
            title_elem = soup.select_one('.p24-title, h1')
            details['title'] = title_elem.get_text(strip=True) if title_elem else 'N/A'
            
            price_elem = soup.select_one('.p24-price')
            details['price_raw'] = price_elem.get_text(strip=True) if price_elem else 'N/A'
            
            desc_elem = soup.select_one('.p24-description, #description')
            details['description'] = desc_elem.get_text(strip=True) if desc_elem else 'N/A'
            
            return details
        except Exception as e:
            logger.error(f"Error extracting listing details from {listing_url}: {e}")
            return None
