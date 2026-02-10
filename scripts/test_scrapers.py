import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.extractors import BuyRentKenyaScraper, Property24Scraper, PigiameScraper, HaoFinderScraper
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_scraper(scraper_class, scraper_name, max_pages=2):
    logger.info(f"\n{'='*50}")
    logger.info(f"Testing {scraper_name}")
    logger.info(f"{'='*50}")
    
    try:
        scraper = scraper_class()
        listings = scraper.extract_listings(max_pages=max_pages)
        
        logger.info(f"Successfully extracted {len(listings)} listings from {scraper_name}")
        
        if listings:
            logger.info(f"\nSample listing from {scraper_name}:")
            sample = listings[0]
            for key, value in sample.items():
                if key != 'scraped_at':
                    logger.info(f"  {key}: {value}")
        
        return len(listings)
        
    except Exception as e:
        logger.error(f"Error testing {scraper_name}: {e}")
        return 0

def main():
    logger.info("Starting scraper tests")
    
    results = {
        'BuyRentKenya': test_scraper(BuyRentKenyaScraper, 'BuyRentKenya', max_pages=1),
        'Property24': test_scraper(Property24Scraper, 'Property24', max_pages=1),
        'PigiaMe': test_scraper(PigiameScraper, 'PigiaMe', max_pages=1),
        'HaoFinder': test_scraper(HaoFinderScraper, 'HaoFinder', max_pages=1),
    }
    
    logger.info(f"\n{'='*50}")
    logger.info("Test Results Summary")
    logger.info(f"{'='*50}")
    
    total = 0
    for site, count in results.items():
        logger.info(f"{site}: {count} listings")
        total += count
    
    logger.info(f"\nTotal listings extracted: {total}")

if __name__ == "__main__":
    main()