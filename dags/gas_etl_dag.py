from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import http.client 
import json
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv(("/home/murae/gasprices_etl/.env"))

def extract_data(**kwargs):
    conn = http.client.HTTPSConnection("api.collectapi.com")
    
    api_key = os.getenv("GAS_API_KEY")

    headers = {
        "content-type": "application/json",
        "authorization": api_key
    }
    conn.request("GET", "/gasPrice/stateUsaPrice?state=WA", headers=headers)
    
    response = conn.getresponse()
    data = response.read().decode("utf-8")
    
    conn.close()

    return data

def transform_data(**kwargs): #pulls raw data from Xcom, cleans & transforms, returns a list of dictionaries
    # Get task instance
    ti = kwargs["ti"]

    raw_data = ti.xcom_pull(task_ids="extract_task")
    data_dict = json.loads(raw_data)
    city_data = data_dict["result"]["cities"]
    city_df = pd.DataFrame(city_data)

    city_df.drop(columns=["lowername"], inplace=True)
    city_df.rename(columns={"name": "cities"}, inplace=True)

    return city_df.to_dict(orient="records")

def load_data(**kwargs): #pulls transformed data from xcom and loads it into postgres
    ti = kwargs["ti"]

    records = ti.xcom_pull(task_ids="transform_task")

    city_df = pd.DataFrame(records)

    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    # Create database engine
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    # Load data into PostgreSQL
    city_df.to_sql("gas_prices", con=engine, schema="public", if_exists="replace", index=False)

    print("Data loaded successfully.")

with DAG(
    dag_id="gas_etl_dag",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "gas_prices"],
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task