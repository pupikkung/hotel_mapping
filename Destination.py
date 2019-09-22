import datetime
from multiprocessing import Process

import numpy as np
import pandas as pd
import pyodbc
from pandas import ExcelWriter

from ConnectorUtility import get_act_connection, get_adaptor_connection


class Destination:
    def run(self):
        # File configuration
        destination_mapping_result_path = 'destination_mapping_result.csv'
        agoda_wholesale_id = 2
        defautTimezone = 13
        insertDateTime = datetime.datetime.now()

        print("Welcome to the show!")
        # remove duplicate 21237
        dataframeMappingDestination = pd.read_csv(destination_mapping_result_path, encoding='utf-8', low_memory=False)

        connAct = get_act_connection()
        connAdaptor = get_adaptor_connection()

        cursorAct = connAct.cursor()
        cursorAct.fast_executemany = True

        cursorAdaptor = connAdaptor.cursor()
        cursorAdaptor.fast_executemany = True

        print("found mapping %r " % dataframeMappingDestination.shape[0])
        dataframeMappingDestination["country_destination"] = dataframeMappingDestination["ACTCountryID"].map(str) + \
                                                             dataframeMappingDestination["AgodaDestinationNameEN"]
        actMissingDf = dataframeMappingDestination[pd.isnull(dataframeMappingDestination['ACTDestinationID'])]
        print("not found in act  %r " % actMissingDf.shape[0])
        # Need to insert to ACT
        val = []
        for label, row in actMissingDf.iterrows():
            val.append((
                row["ACTCountryID"],
                row["AgodaDestinationNameEN"],
                row["AgodaDestinationNameEN"],
                defautTimezone,
                None,  # set keyword as null
                1,  # Active
                insertDateTime,
                insertDateTime
            ))
        sql = "INSERT INTO Destination (CountryID, TitleEN, TitleTH, TimeZoneHour, Keywords, Active, CreatedAt, UpdatedAt) " \
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        # cursorAct.executemany(sql, val)
        # connAct.commit()

        selectDestinationSql = "SELECT DestinationID, CountryID as ACTCountryID, TitleEN as AgodaDestinationNameEN FROM Destination WHERE CreatedAt > ?"
        today = datetime.date.today()
        # today = datetime.date(2019, 9, 21)
        destinationInsertedDf = pd.read_sql_query(selectDestinationSql, connAct, params=(today,))
        destinationInsertedDf["country_destination"] = destinationInsertedDf["ACTCountryID"].map(str) + \
                                                       destinationInsertedDf["AgodaDestinationNameEN"]
        cursorAct.close()
        connAct.close()
        # fill act destination id
        tempdf = dataframeMappingDestination['country_destination'].map(
            destinationInsertedDf.set_index(['country_destination'])['DestinationID'])

        del dataframeMappingDestination['country_destination']
        dataframeMappingDestination = dataframeMappingDestination.join(tempdf)
        dataframeMappingDestination.loc[
            pd.isnull(dataframeMappingDestination['ACTDestinationID']), 'ACTDestinationID'] = \
        dataframeMappingDestination['country_destination']
        dataframeMappingDestination.loc[
            pd.isnull(dataframeMappingDestination['ACTDesintationNameEN']), 'ACTDesintationNameEN'] = \
            dataframeMappingDestination['AgodaDestinationNameEN']
        print(dataframeMappingDestination)

        # Insert agoda destination to adaptor db
        filtered_df = dataframeMappingDestination[dataframeMappingDestination['AgodaDestinationID'].notnull()]
        filtered_df = filtered_df.fillna(value=0)
        filtered_df.drop_duplicates(subset='AgodaDestinationID', keep='last', inplace=True)
        # do multiprocessing
        process_1 = Process(target=self.insert_data, args=[filtered_df, connAdaptor, agoda_wholesale_id])
        del dataframeMappingDestination['country_destination']
        process_2 = Process(target=self.export_excel, args=[dataframeMappingDestination])
        process_1.start()
        process_2.start()
        process_1.join()
        process_2.join()

    def insert_data(self, filtered_df: pd.DataFrame, connAdaptor, agoda_wholesale_id):
        print('Start insert_data...')
        val = []
        for label, row in filtered_df.iterrows():
            val.append((
                int(row["AgodaDestinationID"]),
                int(row["ACTDestinationID"]),
                float(row["AgodaLatitude"]),  # Sungkai invalid row
                float(row["AgodaLongitude"]),
                row["AgodaDestinationNameEN"],
                row["AgodaCountryID"],
                agoda_wholesale_id
            ))
        sql = "INSERT INTO WholesaleDestination (WholesaleDestinationID, DestinationID, Latitude, Longtitude, NameEN, WholesaleCountryID, WholesaleID) " \
              "VALUES (?, ?, ?, ?, ?, ?, ?)"
        cursorAdaptor = connAdaptor.cursor()
        cursorAdaptor.fast_executemany = True

        cursorAdaptor.executemany(sql, val)
        connAdaptor.commit()
        cursorAdaptor.close()
        connAdaptor.close()
        print('End insert_data')

    def export_excel(self, dataframeMappingDestination):
        print('Start export_excel...')
        writer = ExcelWriter('MapDestination.xlsx')
        dataframeMappingDestination.to_excel(writer, 'Map Result', index=False)
        writer.save()
        print('End export_excel')
