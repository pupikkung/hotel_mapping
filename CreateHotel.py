import pandas as pd
import pyodbc


class CreateHotel:
    def run(self, all_hotel_feed_df: pd.DataFrame):
        agoda_hotel_result_path = 'agoda_hotel_en.csv'
        agoda_wholesale_id = 2

        print("Welcome to the show!")

        # dataframeAagodaHotel = pd.read_csv(agoda_hotel_result_path, encoding='utf-8', low_memory=False)