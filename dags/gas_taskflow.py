from airflow.decorators import dag, task
from datetime import datetime
import requests
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv("/home/murae/gasprices_etl/.env")

@dag(
    dag_id="gas_prices_taskflow",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["gas_prices", "taskflow"]
)

def gas_prices_etl():

    @task
    def extract_data():
        url = "https://api.collectapi.com/gasPrice/stateUsaPrice?state=WA"

        headers = {"content-type": "application/json", "authorization": os.getenv("GAS_API_KEY")}

        response = requests.get(url, headers=headers)

        data = response.json()

        return data

    @task
    def transform_data(data):
 
        city_data = data["result"]["cities"]

        city_df = pd.DataFrame(city_data)

        city_df.drop(columns=["lowername"], inplace=True)

        city_df.rename(columns={"name": "cities"}, inplace=True)

        city_df = city_df.astype(object).where(pd.notna(city_df), None)

        gas_records = city_df.to_dict(orient="records")

        return gas_records

    @task
    def load_data(transformed_data):

        city_df = pd.DataFrame(transformed_data)

        DB_NAME = os.getenv("DB_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")

        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

        #test database connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1;"))
            for row in result:
                print(row)

        #load data into postgres
        city_df.to_sql("gas_prices", con=engine, schema="public", if_exists="replace", index=False)

    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)

gas_prices_etl()