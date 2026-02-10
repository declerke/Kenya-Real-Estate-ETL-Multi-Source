from typing import Optional, Dict, List
from bs4 import BeautifulSoup
import logging
from scripts.extractors.base_scraper import BaseScraper
from config.settings import SITE_CONFIGS
from datetime import datetime

logger = logging.getLogger(__name__)

class PigiameScraper(BaseScraper):
    def __init__(self):
        super().__init__('pigiame')
        self.config = SITE_CONFIGS['pigiame']
        
    def extract_listings(self, max_pages: int = 5) -> List[Dict]:
        all_listings = []
        
        for page_num in range(1, max_pages + 1):
            # 2026 UPDATE: Confirming ?page= parameter
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
        # 2026 UPDATE: PigiaMe now uses article tags with listing-card or specific classes
        listing_cards = soup.select('article.listing-card') or soup.find_all('article', class_='listing')
        
        if not listing_cards:
            # Fallback for classified layout
            listing_cards = soup.select('.listings-cards__list-item') or soup.find_all('div', class_='classified-item')
        
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
            # 2026 UPDATE: Class names often use BEM or hyphenated styles now
            title_elem = card.select_one('.listing-card__title, .listing-title') or card.find('h3')
            title = title_elem.get_text(strip=True) if title_elem else 'N/A'
            
            link_elem = card.find('a', href=True)
            if link_elem:
                listing_url = link_elem['href']
                if not listing_url.startswith('http'):
                    listing_url = self.config['base_url'].rstrip('/') + '/' + listing_url.lstrip('/')
            else:
                return None
            
            price_elem = card.select_one('.listing-card__price, .listing-price, .price')
            price_raw = price_elem.get_text(strip=True) if price_elem else 'N/A'
            
            location_elem = card.select_one('.listing-card__location, .listing-location, .location')
            location_raw = location_elem.get_text(strip=True) if location_elem else 'N/A'
            
            # Metadata for PigiaMe is often in a single info container
            info_text = ""
            info_container = card.select_one('.listing-card__info, .listing-details')
            if info_container:
                info_text = info_container.get_text(strip=True).lower()
            
            description_elem = card.select_one('.listing-card__description, .listing-description')
            description = description_elem.get_text(strip=True) if description_elem else 'N/A'
            
            bedrooms_raw = 'N/A'
            bathrooms_raw = 'N/A'
            area_raw = 'N/A'
            
            # Dynamic parsing of the info text
            if info_text:
                parts = info_text.split()
                for i, part in enumerate(parts):
                    if 'bed' in part and i > 0:
                        bedrooms_raw = f"{parts[i-1]} beds"
                    elif 'bath' in part and i > 0:
                        bathrooms_raw = f"{parts[i-1]} baths"
                    elif 'm²' in part or 'sqm' in part:
                        area_raw = f"{parts[i-1]} sqm"
            
            property_type_elem = card.select_one('.listing-card__category, .category')
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
            # 2026 UPDATE: Individual listing detail page classes
            title_elem = soup.select_one('.listing-item__title, .classified-title, h1')
            details['title'] = title_elem.get_text(strip=True) if title_elem else 'N/A'
            
            price_elem = soup.select_one('.listing-item__price, .classified-price')
            details['price_raw'] = price_elem.get_text(strip=True) if price_elem else 'N/A'
            
            desc_elem = soup.select_one('.listing-item__description, .classified-description')
            details['description'] = desc_elem.get_text(strip=True) if desc_elem else 'N/A'
            
            return details
        except Exception as e:
            logger.error(f"Error extracting listing details: {e}")
            return None
