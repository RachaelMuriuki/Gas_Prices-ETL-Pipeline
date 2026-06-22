from extract import extract_data
from transform import transform_data
from load import load_data

def main():
    data = extract_data()
    city_df = transform_data(data)
    load_data(city_df)

    print('Gas_prices ETL completed successfully.')

if __name__ == "__main__":
    main()