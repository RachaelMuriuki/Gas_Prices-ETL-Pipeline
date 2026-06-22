import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def load_data(city_df):
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PORT = os.getenv('DB_PORT')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')

    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}') 

    city_df.to_sql("gas_prices", engine, schema='public', if_exists="replace", index=False)