import pandas as pd
import pyodbc

# Database configuration
from pandas import ExcelWriter

server = 'localhost'
database = 'act'
username = 'SA'
password = 'test1234*'

connACT = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=act' + ';UID=' + username + ';PWD=' + password)

connAdaptor = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=adaptor' + ';UID=' + username + ';PWD=' + password)

# File configuration
agoda_hotel_result_path = 'agoda_hotel_en.csv'
agoda_wholesale_id = 2

print("Welcome to the show!")

dataframeAagodaHotel = pd.read_csv(agoda_hotel_result_path, encoding='utf-8', low_memory=False)

SQL_Query = pd.read_sql_query(
    '''select HotelID, DestinationID, NameEN as hotel_name from Hotel''', connACT)

dataframeACTHotel = pd.DataFrame(SQL_Query, columns=['HotelID', 'DestinationID', 'hotel_name'])

df_inner = pd.merge(dataframeACTHotel[
                        ['HotelID', 'DestinationID', 'hotel_name']],
                    dataframeAagodaHotel[['hotel_id', 'hotel_name']],
                    on='hotel_name', how='inner')

writer = ExcelWriter('Existing_Hotel.xlsx')
df_inner.to_excel(writer, 'Sheet1', index=False)
writer.save()

exit()
