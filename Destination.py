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
destination_mapping_result_path = 'destination_mapping_result.csv'
agoda_wholesale_id = 2

print("Welcome to the show!")
# remove duplicate 21237
dataframeMappingDestination = pd.read_csv(destination_mapping_result_path, encoding='utf-8', low_memory=False)

crsr = connAdaptor.cursor()
crsr.fast_executemany = True

print(dataframeMappingDestination.shape[0])
dataframeMappingDestination = dataframeMappingDestination[pd.notnull(dataframeMappingDestination['ACTDestinationID'])]

dataframeMappingDestination = dataframeMappingDestination[pd.notnull(dataframeMappingDestination['AgodaDestinationID'])]
print(dataframeMappingDestination.shape[0])

dataframeMappingDestination = dataframeMappingDestination.fillna(value=0)

# dropping duplicate values
dataframeMappingDestination.drop_duplicates(subset='AgodaDestinationID', keep='last', inplace=True)
print(dataframeMappingDestination.shape[0])

sql = "INSERT INTO WholesaleDestination (WholesaleDestinationID, DestinationID, Latitude, Longtitude, NameEN, WholesaleCountryID, WholesaleID) " \
      "VALUES (?, ?, ?, ?, ?, ?, ?)"

val = []
for label, row in dataframeMappingDestination.iterrows():
    val.append((
        row["AgodaDestinationID"],
        row["ACTDestinationID"],
        row["AgodaLatitude"],
        row["AgodaLongitude"],
        row["AgodaDestinationNameEN"],
        row["AgodaCountryID"],
        agoda_wholesale_id
    ))

print(val)

crsr.executemany(sql, val)
connAdaptor.commit()
print(crsr.rowcount, "was inserted. (Destinations)")
crsr.close()
connAdaptor.close()

exit()
