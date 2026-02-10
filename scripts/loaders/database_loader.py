import pandas as pd
import logging
from sqlalchemy.orm import Session
from sqlalchemy import select
from config.database import RawListing, CleanedListing, get_session
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseLoader:
    def __init__(self):
        self.session = get_session()
        
    def load_raw_listings(self, listings_data: list) -> int:
        logger.info(f"Loading {len(listings_data)} raw listings to database")
        
        inserted_count = 0
        skipped_count = 0
        
        for listing in listings_data:
            try:
                existing = self.session.query(RawListing).filter(
                    RawListing.listing_url == listing['listing_url']
                ).first()
                
                if existing:
                    skipped_count += 1
                    continue
                
                raw_listing = RawListing(
                    source_site=listing.get('source_site'),
                    listing_url=listing.get('listing_url'),
                    title=listing.get('title'),
                    description=listing.get('description'),
                    price_raw=listing.get('price_raw'),
                    location_raw=listing.get('location_raw'),
                    bedrooms_raw=listing.get('bedrooms_raw'),
                    bathrooms_raw=listing.get('bathrooms_raw'),
                    area_raw=listing.get('area_raw'),
                    property_type_raw=listing.get('property_type_raw'),
                    scraped_at=listing.get('scraped_at', datetime.utcnow())
                )
                
                self.session.add(raw_listing)
                inserted_count += 1
                
            except Exception as e:
                logger.error(f"Error inserting raw listing: {e}")
                self.session.rollback()
                continue
        
        try:
            self.session.commit()
            logger.info(f"Successfully loaded {inserted_count} raw listings, skipped {skipped_count} duplicates")
        except Exception as e:
            logger.error(f"Error committing raw listings: {e}")
            self.session.rollback()
            
        return inserted_count
    
    def load_cleaned_listings(self, df: pd.DataFrame) -> int:
        logger.info(f"Loading {len(df)} cleaned listings to database")
        
        inserted_count = 0
        skipped_count = 0
        
        for _, row in df.iterrows():
            try:
                existing = self.session.query(CleanedListing).filter(
                    CleanedListing.listing_url == row['listing_url']
                ).first()
                
                if existing:
                    skipped_count += 1
                    continue
                
                raw_listing = self.session.query(RawListing).filter(
                    RawListing.listing_url == row['listing_url']
                ).first()
                
                raw_listing_id = raw_listing.id if raw_listing else None
                
                cleaned_listing = CleanedListing(
                    raw_listing_id=raw_listing_id,
                    source_site=row.get('source_site'),
                    listing_url=row.get('listing_url'),
                    title=row.get('title'),
                    description=row.get('description'),
                    price_kes=float(row['price_kes']) if pd.notna(row.get('price_kes')) else None,
                    county=row.get('county'),
                    neighborhood=row.get('neighborhood'),
                    bedrooms=int(row['bedrooms']) if pd.notna(row.get('bedrooms')) else None,
                    bathrooms=int(row['bathrooms']) if pd.notna(row.get('bathrooms')) else None,
                    area_sqm=float(row['area_sqm']) if pd.notna(row.get('area_sqm')) else None,
                    property_type=row.get('property_type'),
                    scraped_at=row.get('scraped_at', datetime.utcnow()),
                    cleaned_at=datetime.utcnow()
                )
                
                self.session.add(cleaned_listing)
                inserted_count += 1
                
            except Exception as e:
                logger.error(f"Error inserting cleaned listing: {e}")
                self.session.rollback()
                continue
        
        try:
            self.session.commit()
            logger.info(f"Successfully loaded {inserted_count} cleaned listings, skipped {skipped_count} duplicates")
        except Exception as e:
            logger.error(f"Error committing cleaned listings: {e}")
            self.session.rollback()
            
        return inserted_count
    
    def get_statistics(self) -> dict:
        try:
            raw_count = self.session.query(RawListing).count()
            cleaned_count = self.session.query(CleanedListing).count()
            
            sites = self.session.query(RawListing.source_site).distinct().all()
            site_names = [site[0] for site in sites]
            
            return {
                'total_raw_listings': raw_count,
                'total_cleaned_listings': cleaned_count,
                'active_sources': site_names
            }
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}
    
    def close(self):
        self.session.close()