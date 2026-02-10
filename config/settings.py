import os
from dotenv import load_dotenv

load_dotenv()

SCRAPING_CONFIG = {
    'delay_min': int(os.getenv('SCRAPING_DELAY_MIN', 2)),
    'delay_max': int(os.getenv('SCRAPING_DELAY_MAX', 5)),
    'timeout': int(os.getenv('REQUEST_TIMEOUT', 30)),
    # Updated User-Agent to avoid basic bot detection on Kenyan portals
    'user_agent': os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'),
}

SITE_CONFIGS = {
    'buyrentkenya': {
        'base_url': 'https://www.buyrentkenya.com',
        'search_url': 'https://www.buyrentkenya.com/property-for-sale', # Updated from /discover
        'max_pages': 10,
        'enabled': True,
    },
    'property24': {
        'base_url': 'https://www.property24.co.ke',
        'search_url': 'https://www.property24.co.ke/property-for-sale',
        'max_pages': 10,
        'enabled': True,
    },
    'pigiame': {
        'base_url': 'https://www.pigiame.co.ke',
        'search_url': 'https://www.pigiame.co.ke/houses-for-sale', # Updated from /housing-real-estate
        'max_pages': 10,
        'enabled': True,
    },
    'haofinder': {
        'base_url': 'https://www.haofinder.com',
        'search_url': 'https://www.haofinder.com/properties', # Updated from /property-for-sale-in-kenya
        'max_pages': 10,
        'enabled': True,
    },
}

KENYAN_COUNTIES = [
    'Nairobi', 'Mombasa', 'Kisumu', 'Nakuru', 'Kiambu', 'Machakos', 
    'Kajiado', 'Uasin Gishu', 'Meru', 'Nyeri', 'Nyandarua', 'Kirinyaga',
    'Murang\'a', 'Embu', 'Tharaka Nithi', 'Kitui', 'Makueni', 'Kilifi',
    'Kwale', 'Lamu', 'Taita Taveta', 'Garissa', 'Wajir', 'Mandera',
    'Marsabit', 'Isiolo', 'Turkana', 'West Pokot', 'Samburu', 'Trans Nzoia',
    'Bungoma', 'Busia', 'Kakamega', 'Vihiga', 'Siaya', 'Kisii', 'Nyamira',
    'Homa Bay', 'Migori', 'Kericho', 'Bomet', 'Nandi', 'Baringo', 'Laikipia',
    'Elgeyo Marakwet', 'Narok', 'Tana River'
]

LOCATION_MAPPINGS = {
    # Nairobi
    'westlands': {'county': 'Nairobi', 'neighborhood': 'Westlands'},
    'kilimani': {'county': 'Nairobi', 'neighborhood': 'Kilimani'},
    'lavington': {'county': 'Nairobi', 'neighborhood': 'Lavington'},
    'karen': {'county': 'Nairobi', 'neighborhood': 'Karen'},
    'runda': {'county': 'Nairobi', 'neighborhood': 'Runda'},
    'kileleshwa': {'county': 'Nairobi', 'neighborhood': 'Kileleshwa'},
    'parklands': {'county': 'Nairobi', 'neighborhood': 'Parklands'},
    'upperhill': {'county': 'Nairobi', 'neighborhood': 'Upper Hill'},
    'upper hill': {'county': 'Nairobi', 'neighborhood': 'Upper Hill'},
    'south c': {'county': 'Nairobi', 'neighborhood': 'South C'},
    'south b': {'county': 'Nairobi', 'neighborhood': 'South B'},
    'syokimau': {'county': 'Machakos', 'neighborhood': 'Syokimau'},
    'kitengela': {'county': 'Kajiado', 'neighborhood': 'Kitengela'},
    
    # Coast
    'nyali': {'county': 'Mombasa', 'neighborhood': 'Nyali'},
    'bamburi': {'county': 'Mombasa', 'neighborhood': 'Bamburi'},
    'diani': {'county': 'Kwale', 'neighborhood': 'Diani'},
    
    # Others
    'kisumu': {'county': 'Kisumu', 'neighborhood': 'Kisumu CBD'},
    'eldoret': {'county': 'Uasin Gishu', 'neighborhood': 'Eldoret'},
    'thika': {'county': 'Kiambu', 'neighborhood': 'Thika'},
    'ruaka': {'county': 'Kiambu', 'neighborhood': 'Ruaka'},
    'kiambu': {'county': 'Kiambu', 'neighborhood': 'Kiambu Town'},
    'nakuru': {'county': 'Nakuru', 'neighborhood': 'Nakuru Town'},
}

PROPERTY_TYPE_MAPPINGS = {
    'apartment': 'Apartment',
    'flat': 'Apartment',
    'studio': 'Studio',
    'bedsitter': 'Studio',
    'house': 'House',
    'bungalow': 'House',
    'mansion': 'House',
    'maisonette': 'Maisonette',
    'villa': 'Villa',
    'townhouse': 'Townhouse',
    'land': 'Land',
    'plot': 'Land',
    'commercial': 'Commercial',
    'office': 'Commercial',
    'warehouse': 'Commercial',
}