import pandas as pd
import pyodbc
from numpy import float64
from pandas import ExcelWriter

from ConnectorUtility import get_adaptor_connection


class Hotel:
    def run(self):
        # File configuration
        hotel_mapping_result_path = 'hotel_mapping_v2.csv'
        agoda_wholesale_id = 2

        print("Welcome to the show!")
        dataframeMappingHotel = pd.read_csv(hotel_mapping_result_path, encoding='utf-8', low_memory=False)

        print("found mapping %r " % dataframeMappingHotel.shape[0])
        dataframeMappingHotel = dataframeMappingHotel[pd.notnull(dataframeMappingHotel['AgodaHotelID'])]
        dataframeMappingHotel = dataframeMappingHotel[pd.notnull(dataframeMappingHotel['ACTHotelID'])]
        # dataframeMappingHotel = dataframeMappingHotel.fillna(value=0)
        dataframeMappingHotel.drop_duplicates(subset='AgodaHotelID', keep='last', inplace=True)
        print("exist in act  %r " % dataframeMappingHotel.shape[0])

        print('Start insert_data...')
        val = []
        for label, row in dataframeMappingHotel.iterrows():
            val.append((
                row["AgodaHotelID"],
                None,
                None,
                row["ACTHotelID"],
                row["AgodaLatitude"],
                row["AgodaLongitude"],
                row["AgodaHotelName"],
                None,
                None,
                row["AgodaDestinationID"],
                agoda_wholesale_id
            ))
        sql = "INSERT INTO WholesaleHotel (WholesaleHotelID, AddressEN, DetailEN, HotelID, Latitude, Longtitude, " \
              "NameEN, Phone, PostCode, WholesaleDestinationID, WholesaleID) " \
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        connAdaptor = get_adaptor_connection()
        cursorAdaptor = connAdaptor.cursor()
        cursorAdaptor.fast_executemany = True

        cursorAdaptor.executemany(sql, val)
        connAdaptor.commit()
        cursorAdaptor.close()
        connAdaptor.close()
        print('End insert_data')
