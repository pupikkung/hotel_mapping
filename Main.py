import math

import pyodbc
import pandas as pd
import openpyxl

# Database configuration
from pandas import ExcelWriter

server = 'localhost'
database = 'act'
username = 'SA'
password = 'test1234*'
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

connAdaptor = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=adaptor' + ';UID=' + username + ';PWD=' + password)

# File configuration
country_mapping_result_path = 'country_mapping_result.csv'
destination_mapping_result_path = 'destination_mapping_result.csv'
agoda_hotel_en_path = 'agoda_hotel_en'
agoda_wholesale_id = 2

print("Welcome to the show!")

# cursor = conn.cursor()
# cursor.execute('SELECT * FROM Country')
# for row in cursor:
#     print('row = %r' % (row,))

SQL_Query = pd.read_sql_query(
    '''select
CountryID ACTCountryID,
ISO2,
ISO3,
TitleEn,
TitleTh,
Slug,
2 as WholeSaleId
from Country''', conn)

# dataframeCountryACT = pd.DataFrame(SQL_Query, columns=['ACTCountryID', 'ISO2', 'ISO3', 'TitleEn', 'TitleTh', 'Slug',
#                                                        'WholeSaleId'])
# print(dataframeCountryACT)

dataframeMappingCountry = pd.read_csv(country_mapping_result_path, encoding='utf-8')

# dataframeMappingDestination = pd.read_csv(destination_mapping_result_path, encoding='utf-8')
# print(dataframeMappingDestination.head())

crsr = connAdaptor.cursor()
crsr.fast_executemany = True

sql = "INSERT INTO WholesaleCountry (WholesaleCountryID, CountryID, ISO2, ISO3, Latitude, Longtitude, NameEN, WholesaleID) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
# val = df_inner[['ACTCountryID', 'ISO2', 'ISO3', 'AgodaLatitude', 'AgodaLongtitude', 'AgodaNameEN', 'WholeSaleId']]

# Drop rows with any empty cells
print(dataframeMappingCountry.shape[0])
dataframeMappingCountry = dataframeMappingCountry[pd.notnull(dataframeMappingCountry['AgodaCountryID'])]
print(dataframeMappingCountry.shape[0])
# writer = ExcelWriter('Pandas-Example2.xlsx')
# dataframeMappingCountry.to_excel(writer,'Sheet1',index=False)
# writer.save()
dataframeMappingCountry = dataframeMappingCountry.fillna(value=0)
val = []
for label, row in dataframeMappingCountry.iterrows():
    val.append((
        row["AgodaCountryID"],
        row["ACTCountryID"],
        row["AgodaISO2"],
        row["AgodaISO3"],
        row["AgodaLatitude"],
        row["AgodaLongtitude"],
        row["AgodaNameEN"],
        agoda_wholesale_id
    ))

crsr.executemany(sql, val)
connAdaptor.commit()
print(crsr.rowcount, "was inserted. (Countries)")
crsr.close()
connAdaptor.close()


exit()
