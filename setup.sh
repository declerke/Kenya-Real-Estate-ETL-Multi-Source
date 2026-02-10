#!/bin/bash

echo "========================================"
echo "Kenya Real Estate ETL - Quick Start"
echo "========================================"
echo ""

echo "Step 1: Create virtual environment"
python3 -m venv venv
source venv/bin/activate

echo ""
echo "Step 2: Install dependencies"
pip install -r requirements.txt

echo ""
echo "Step 3: Set up environment variables"
cp .env.example .env
echo "Please edit .env now with your database credentials"
read -p "Press enter when done..."

echo ""
echo "Step 4: Initialize database"
python scripts/init_database.py

echo ""
echo "Step 5: Test scrapers (optional)"
read -p "Do you want to test scrapers? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    python scripts/test_scrapers.py
fi

echo ""
echo "Step 6: Set up Airflow"
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo ""
echo "Step 7: Copy DAG to Airflow"
mkdir -p $AIRFLOW_HOME/dags
cp dags/kenya_real_estate_dag.py $AIRFLOW_HOME/dags/

echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "To start Airflow:"
echo "  Terminal 1: airflow webserver --port 8080"
echo "  Terminal 2: airflow scheduler"
echo ""
echo "Access Airflow UI at: http://localhost:8080"
echo "Username: admin"
echo "Password: admin"
echo ""
echo "To run the pipeline manually:"
echo "  airflow dags trigger kenya_real_estate_etl_multi_source"
echo ""
echo "To analyze data:"
echo "  jupyter notebook notebooks/market_analysis.ipynb"
echo ""
