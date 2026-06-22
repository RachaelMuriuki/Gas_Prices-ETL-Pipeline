import os
import json
import pandas as pd
import http.client 
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def extract_data():
    conn = http.client.HTTPSConnection("api.collectapi.com")

    headers = {
         'content-type': "application/json",
         'authorization': os.getenv("GAS_API_KEY")
        }

    conn.request("GET", "/gasPrice/stateUsaPrice?state=WA", headers=headers)

    res = conn.getresponse()
    data = res.read()

    return data

def transform_data(data):
    data_dict = json.loads(data.decode("utf-8"))

    city_data = data_dict["result"]["cities"]

    city_df = pd.DataFrame(city_data)

    city_df.drop(columns=["lowername"], inplace=True)

    city_df.rename(columns={"name": "cities"}, inplace=True)

    return city_df

def load_data(city_df):
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PORT = os.getenv('DB_PORT')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')

    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}') 

    city_df.to_sql("gas_prices", engine, schema='public', if_exists="replace", index=False)

def main():
    data = extract_data()
    city_df = transform_data(data)
    load_data(city_df)

    print('Gas_prices ETL completed successfully.')

if __name__ == "__main__":
    main()