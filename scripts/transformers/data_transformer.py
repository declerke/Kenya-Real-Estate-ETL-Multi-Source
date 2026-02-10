import pandas as pd
import re
import logging
from typing import Optional
from config.settings import LOCATION_MAPPINGS, PROPERTY_TYPE_MAPPINGS, KENYAN_COUNTIES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self):
        self.location_mappings = LOCATION_MAPPINGS
        self.property_type_mappings = PROPERTY_TYPE_MAPPINGS
        self.kenyan_counties = KENYAN_COUNTIES
        
    def transform_listings(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Starting transformation of {len(df)} listings")
        
        df['price_kes'] = df['price_raw'].apply(self._parse_price)
        
        location_info = df['location_raw'].apply(self._parse_location)
        df['county'] = location_info.apply(lambda x: x['county'])
        df['neighborhood'] = location_info.apply(lambda x: x['neighborhood'])
        
        df['bedrooms'] = df['bedrooms_raw'].apply(self._parse_number)
        df['bathrooms'] = df['bathrooms_raw'].apply(self._parse_number)
        df['area_sqm'] = df['area_raw'].apply(self._parse_area)
        df['property_type'] = df['property_type_raw'].apply(self._standardize_property_type)
        
        df = df.dropna(subset=['price_kes'])
        df = df[df['price_kes'] > 0]
        
        logger.info(f"Transformation complete. {len(df)} listings after cleaning")
        return df
    
    def _parse_price(self, price_str: str) -> Optional[float]:
        if pd.isna(price_str) or price_str == 'N/A':
            return None
            
        try:
            price_str = str(price_str).upper().strip()
            
            price_str = re.sub(r'[^\d\.KMB]', '', price_str)
            
            multiplier = 1
            if 'M' in price_str:
                multiplier = 1_000_000
                price_str = price_str.replace('M', '')
            elif 'K' in price_str:
                multiplier = 1_000
                price_str = price_str.replace('K', '')
            elif 'B' in price_str:
                multiplier = 1_000_000_000
                price_str = price_str.replace('B', '')
            
            price_num = float(price_str)
            return price_num * multiplier
            
        except (ValueError, AttributeError) as e:
            logger.debug(f"Could not parse price: {price_str}")
            return None
    
    def _parse_location(self, location_str: str) -> dict:
        if pd.isna(location_str) or location_str == 'N/A':
            return {'county': None, 'neighborhood': None}
            
        try:
            location_str = str(location_str).strip().lower()
            
            for key, value in self.location_mappings.items():
                if key in location_str:
                    return value
            
            for county in self.kenyan_counties:
                if county.lower() in location_str:
                    return {'county': county, 'neighborhood': location_str.title()}
            
            parts = location_str.split(',')
            if len(parts) >= 2:
                neighborhood = parts[0].strip().title()
                county = parts[1].strip().title()
                for known_county in self.kenyan_counties:
                    if known_county.lower() in county.lower():
                        return {'county': known_county, 'neighborhood': neighborhood}
            
            return {'county': None, 'neighborhood': location_str.title()}
            
        except Exception as e:
            logger.debug(f"Could not parse location: {location_str}")
            return {'county': None, 'neighborhood': None}
    
    def _parse_number(self, num_str: str) -> Optional[int]:
        if pd.isna(num_str) or num_str == 'N/A':
            return None
            
        try:
            num_str = str(num_str).lower()
            numbers = re.findall(r'\d+', num_str)
            if numbers:
                return int(numbers[0])
            return None
        except (ValueError, AttributeError):
            return None
    
    def _parse_area(self, area_str: str) -> Optional[float]:
        if pd.isna(area_str) or area_str == 'N/A':
            return None
            
        try:
            area_str = str(area_str).lower()
            numbers = re.findall(r'\d+\.?\d*', area_str)
            if numbers:
                area = float(numbers[0])
                
                if 'acre' in area_str:
                    area = area * 4046.86
                elif 'hectare' in area_str or 'ha' in area_str:
                    area = area * 10000
                
                return area
            return None
        except (ValueError, AttributeError):
            return None
    
    def _standardize_property_type(self, type_str: str) -> Optional[str]:
        if pd.isna(type_str) or type_str == 'N/A':
            return None
            
        try:
            type_str = str(type_str).lower().strip()
            
            for key, value in self.property_type_mappings.items():
                if key in type_str:
                    return value
            
            return type_str.title()
            
        except Exception as e:
            logger.debug(f"Could not standardize property type: {type_str}")
            return None
    
    def deduplicate_listings(self, df: pd.DataFrame) -> pd.DataFrame:
        initial_count = len(df)
        
        df = df.drop_duplicates(subset=['listing_url'], keep='first')
        
        logger.info(f"Removed {initial_count - len(df)} duplicate listings based on URL")
        return df