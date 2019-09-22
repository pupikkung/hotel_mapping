import pandas as pd
import pyodbc
from numpy import float64
from pandas import ExcelWriter

from ConnectorUtility import get_adaptor_connection


class Hotel:
    def run(self):
        # File configuration
        hotel_mapping_result_path = 'resources/Hotel Mapping Thai only.xlsx'
        agoda_wholesale_id = 2
        htb_wholesale_id = 1

        print("Welcome to the show!")
        dataframeMappingHotelFile = pd.read_excel(hotel_mapping_result_path, sheet_name='Hotel List')

        print("found mapping %r " % dataframeMappingHotelFile.shape[0])
        dataframeMappingAgodaHotel = dataframeMappingHotelFile[pd.notnull(dataframeMappingHotelFile['AgodaHotelID'])]
        dataframeMappingAgodaHotel = dataframeMappingAgodaHotel[pd.notnull(dataframeMappingAgodaHotel['ACTHotelID'])]
        dataframeMappingAgodaHotel = dataframeMappingAgodaHotel[
            pd.notnull(dataframeMappingAgodaHotel['AgodaDestinationID'])]
        dataframeMappingAgodaHotel = dataframeMappingAgodaHotel[
            pd.notnull(dataframeMappingAgodaHotel['AgodaLatitude'])]
        dataframeMappingAgodaHotel.drop_duplicates(subset='AgodaHotelID', keep='last', inplace=True)
        print("agoda hotels exist in act  %r " % dataframeMappingAgodaHotel.shape[0])

        dataframeMappingHTBHotel = dataframeMappingHotelFile[pd.notnull(dataframeMappingHotelFile['HTBHotelID'])]
        dataframeMappingHTBHotel = dataframeMappingHTBHotel[pd.notnull(dataframeMappingHTBHotel['ACTHotelID'])]
        dataframeMappingHTBHotel = dataframeMappingHTBHotel[
            pd.notnull(dataframeMappingHTBHotel['AgodaDestinationID'])]
        dataframeMappingHTBHotel = dataframeMappingHTBHotel[
            pd.notnull(dataframeMappingHTBHotel['HTBLatitude'])]
        dataframeMappingHTBHotel.drop_duplicates(subset='HTBHotelID', keep='last', inplace=True)
        print("htb hotels exist in act  %r " % dataframeMappingHTBHotel.shape[0])

        print('Start insert_data...')
        val = []
        for label, row in dataframeMappingAgodaHotel.iterrows():
            val.append((
                int(row["AgodaHotelID"]),
                None,
                None,
                int(row["ACTHotelID"]),
                float(row["AgodaLatitude"]),
                float(row["AgodaLongitude"]),
                row["AgodaHotelName"],
                None,
                None,
                int(row["AgodaDestinationID"]),
                agoda_wholesale_id
            ))

        val2 = []
        for label, row in dataframeMappingHTBHotel.iterrows():
            val2.append((
                int(row["HTBHotelID"]),
                None,
                None,
                int(row["ACTHotelID"]),
                float(row["HTBLatitude"]),
                float(row["HTBLongitude"]),
                row["HTBHotelName"],
                None,
                None,
                int(row["ACTDestinationID"]),  # TODO need check missing HTB destination ID
                htb_wholesale_id
            ))

        sql = "INSERT INTO WholesaleHotel (WholesaleHotelID, AddressEN, DetailEN, HotelID, Latitude, Longtitude, " \
              "NameEN, Phone, PostCode, WholesaleDestinationID, WholesaleID) " \
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        connAdaptor = get_adaptor_connection()
        cursorAdaptor = connAdaptor.cursor()
        cursorAdaptor.fast_executemany = True

        cursorAdaptor.executemany(sql, val)
        connAdaptor.commit()

        cursorAdaptor.executemany(sql, val2)
        connAdaptor.commit()
        cursorAdaptor.close()
        connAdaptor.close()
        print('End insert_data')
