import pandas as pd
import pyodbc
from numpy import float64
from pandas import ExcelWriter

# Database configuration
server = 'localhost'
database = 'act'
username = 'SA'
password = 'test1234*'

connAdaptor = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=adaptor' + ';UID=' + username + ';PWD=' + password)

# File configuration
hotel_mapping_result_path = 'hotel_mapping_v2.csv'
agoda_wholesale_id = 2

print("Welcome to the show!")

dataframeMappingHotel = pd.read_csv(hotel_mapping_result_path, encoding='utf-8', low_memory=False)
# ACTCountryID	ACTDestinationID	AgodaDestinationID	ACTDestinationName	ACTHotelID	ACTHotelName	AgodaHotelID
# AgodaHotelName	ACTLatitude	ACTLongitude	AgodaLatitude	AgodaLongitude	HTBHotelID	HTBHotelName	HTBLatitude	HTBLongitude

crsr = connAdaptor.cursor()
crsr.fast_executemany = True

print(dataframeMappingHotel.shape[0])
dataframeMappingHotel = dataframeMappingHotel[pd.notnull(dataframeMappingHotel['AgodaHotelID'])]
print(dataframeMappingHotel.shape[0])
dataframeMappingHotel = dataframeMappingHotel[pd.notnull(dataframeMappingHotel['ACTHotelID'])]
print(dataframeMappingHotel.shape[0])

dataframeMappingHotel = dataframeMappingHotel.fillna(value=0)

# dropping duplicate values
dataframeMappingHotel.drop_duplicates(subset='AgodaHotelID', keep='last', inplace=True)
print(dataframeMappingHotel.shape[0])

sql = "INSERT INTO WholesaleHotel (WholesaleHotelID, AddressEN, DetailEN, HotelID, Latitude, Longtitude, " \
      "NameEN, Phone, PostCode, WholesaleDestinationID, WholesaleID) " \
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

val = []
for label, row in dataframeMappingHotel.iterrows():
    val.append((
        row["AgodaHotelID"],
        '',
        '',
        row["ACTHotelID"],
        row["AgodaLatitude"],
        row["AgodaLongitude"],
        row["AgodaHotelName"],
        '',
        '',
        row["AgodaDestinationID"],
        agoda_wholesale_id
    ))

print(val)

crsr.executemany(sql, val)
connAdaptor.commit()
print(crsr.rowcount, "was inserted. (Hotels)")
crsr.close()
connAdaptor.close()

exit()

