import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

load_dotenv()

Base = declarative_base()

class RawListing(Base):
    __tablename__ = 'raw_listings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_site = Column(String(100), nullable=False)
    listing_url = Column(String(500), unique=True, nullable=False)
    title = Column(Text)
    description = Column(Text)
    price_raw = Column(String(200))
    location_raw = Column(String(300))
    bedrooms_raw = Column(String(50))
    bathrooms_raw = Column(String(50))
    area_raw = Column(String(100))
    property_type_raw = Column(String(100))
    scraped_at = Column(DateTime, default=datetime.utcnow)
    
class CleanedListing(Base):
    __tablename__ = 'cleaned_listings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_listing_id = Column(Integer, nullable=False)
    source_site = Column(String(100), nullable=False)
    listing_url = Column(String(500), unique=True, nullable=False)
    title = Column(Text)
    description = Column(Text)
    price_kes = Column(Float)
    county = Column(String(100))
    neighborhood = Column(String(200))
    bedrooms = Column(Integer)
    bathrooms = Column(Integer)
    area_sqm = Column(Float)
    property_type = Column(String(100))
    scraped_at = Column(DateTime)
    cleaned_at = Column(DateTime, default=datetime.utcnow)

def get_database_url():
    host = os.getenv('AIVEN_DB_HOST')
    port = os.getenv('AIVEN_DB_PORT')
    name = os.getenv('AIVEN_DB_NAME')
    user = os.getenv('AIVEN_DB_USER')
    password = os.getenv('AIVEN_DB_PASSWORD')
    
    return f"postgresql://{user}:{password}@{host}:{port}/{name}?sslmode=require"

def get_engine():
    database_url = get_database_url()
    return create_engine(database_url, echo=False, pool_pre_ping=True)

def create_tables():
    engine = get_engine()
    Base.metadata.create_all(engine)
    return engine

def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()