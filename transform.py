import json
import pandas as pd

def transform_data(data):
    data_dict = json.loads(data.decode("utf-8"))

    city_data = data_dict["result"]["cities"]

    city_df = pd.DataFrame(city_data)

    city_df.drop(columns=["lowername"], inplace=True)

    city_df.rename(columns={"name": "cities"}, inplace=True)

    return city_df