from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import sys
import os

win_project_root = "/mnt/c/Users/Administrator/Documents/Luxdev/Kenya-Real-Estate-ETL-Multi-Source"

if win_project_root not in sys.path:
    sys.path.insert(0, win_project_root)

from scripts.extractors import BuyRentKenyaScraper, Property24Scraper, PigiameScraper, HaoFinderScraper
from scripts.transformers import DataTransformer
from scripts.loaders import DatabaseLoader
from config.database import create_tables

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kenya_real_estate_etl_multi_source',
    default_args=default_args,
    description='Multi-source ETL pipeline for Kenyan real estate listings',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['real-estate', 'kenya', 'etl', 'multi-source'],
)

def initialize_database(**context):
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Initializing database tables")
    
    try:
        create_tables()
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise

def extract_buyrentkenya(**context):
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Starting extraction from BuyRentKenya")
    
    scraper = BuyRentKenyaScraper()
    listings = scraper.extract_listings(max_pages=5)
    
    logger.info(f"Extracted {len(listings)} listings from BuyRentKenya")
    context['ti'].xcom_push(key='buyrentkenya_listings', value=listings)
    return len(listings)

def extract_property24(**context):
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Starting extraction from Property24")
    
    scraper = Property24Scraper()
    listings = scraper.extract_listings(max_pages=5)
    
    logger.info(f"Extracted {len(listings)} listings from Property24")
    context['ti'].xcom_push(key='property24_listings', value=listings)
    return len(listings)

def extract_pigiame(**context):
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Starting extraction from PigiaMe")
    
    scraper = PigiameScraper()
    listings = scraper.extract_listings(max_pages=5)
    
    logger.info(f"Extracted {len(listings)} listings from PigiaMe")
    context['ti'].xcom_push(key='pigiame_listings', value=listings)
    return len(listings)

def extract_haofinder(**context):
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Starting extraction from HaoFinder")
    
    scraper = HaoFinderScraper()
    listings = scraper.extract_listings(max_pages=5)
    
    logger.info(f"Extracted {len(listings)} listings from HaoFinder")
    context['ti'].xcom_push(key='haofinder_listings', value=listings)
    return len(listings)

def merge_and_load_raw(**context):
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Merging listings from all sources and loading to raw table")
    
    ti = context['ti']
    
    buyrentkenya_listings = ti.xcom_pull(key='buyrentkenya_listings', task_ids='extract_buyrentkenya') or []
    property24_listings = ti.xcom_pull(key='property24_listings', task_ids='extract_property24') or []
    pigiame_listings = ti.xcom_pull(key='pigiame_listings', task_ids='extract_pigiame') or []
    haofinder_listings = ti.xcom_pull(key='haofinder_listings', task_ids='extract_haofinder') or []
    
    all_listings = buyrentkenya_listings + property24_listings + pigiame_listings + haofinder_listings
    
    logger.info(f"Total listings from all sources: {len(all_listings)}")
    
    if len(all_listings) == 0:
        logger.warning("No listings extracted from any source")
        return 0
    
    loader = DatabaseLoader()
    inserted_count = loader.load_raw_listings(all_listings)
    loader.close()
    
    context['ti'].xcom_push(key='raw_inserted_count', value=inserted_count)
    return inserted_count

def transform_and_load_cleaned(**context):
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Transforming data and loading to cleaned table")
    
    ti = context['ti']
    
    buyrentkenya_listings = ti.xcom_pull(key='buyrentkenya_listings', task_ids='extract_buyrentkenya') or []
    property24_listings = ti.xcom_pull(key='property24_listings', task_ids='extract_property24') or []
    pigiame_listings = ti.xcom_pull(key='pigiame_listings', task_ids='extract_pigiame') or []
    haofinder_listings = ti.xcom_pull(key='haofinder_listings', task_ids='extract_haofinder') or []
    
    all_listings = buyrentkenya_listings + property24_listings + pigiame_listings + haofinder_listings
    
    if len(all_listings) == 0:
        logger.warning("No listings to transform")
        return 0
    
    df = pd.DataFrame(all_listings)
    
    transformer = DataTransformer()
    df_cleaned = transformer.transform_listings(df)
    df_cleaned = transformer.deduplicate_listings(df_cleaned)
    
    loader = DatabaseLoader()
    inserted_count = loader.load_cleaned_listings(df_cleaned)
    
    stats = loader.get_statistics()
    logger.info(f"Database statistics: {stats}")
    
    loader.close()
    
    return inserted_count

init_db_task = PythonOperator(
    task_id='initialize_database',
    python_callable=initialize_database,
    dag=dag,
)

extract_buyrentkenya_task = PythonOperator(
    task_id='extract_buyrentkenya',
    python_callable=extract_buyrentkenya,
    dag=dag,
)

extract_property24_task = PythonOperator(
    task_id='extract_property24',
    python_callable=extract_property24,
    dag=dag,
)

extract_pigiame_task = PythonOperator(
    task_id='extract_pigiame',
    python_callable=extract_pigiame,
    dag=dag,
)

extract_haofinder_task = PythonOperator(
    task_id='extract_haofinder',
    python_callable=extract_haofinder,
    dag=dag,
)

merge_load_raw_task = PythonOperator(
    task_id='merge_and_load_raw',
    python_callable=merge_and_load_raw,
    dag=dag,
)

transform_load_cleaned_task = PythonOperator(
    task_id='transform_and_load_cleaned',
    python_callable=transform_load_cleaned,
    dag=dag,
)

init_db_task >> [extract_buyrentkenya_task, extract_property24_task, extract_pigiame_task, extract_haofinder_task]
[extract_buyrentkenya_task, extract_property24_task, extract_pigiame_task, extract_haofinder_task] >> merge_load_raw_task
merge_load_raw_task >> transform_load_cleaned_task