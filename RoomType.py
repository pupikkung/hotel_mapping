import pyodbc
import pandas as pd
from urllib.request import urlopen
from xml.etree.ElementTree import parse

# Database configuration
server = 'localhost'
database = 'act'
username = 'SA'
password = 'test1234*'
connAdaptor = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=adaptor' + ';UID=' + username + ';PWD=' + password)

# File configuration
feed_url = 'http://affiliatefeed.agoda.com/datafeeds/feed/getfeed?apikey=aaad14ef-e6d9-42ca-80ba-d73a7c82c33e&mhotel_id=%s&feed_id=6'
agoda_wholesale_id = 2

print("Welcome to the show!")

SQL_Query = pd.read_sql_query(
    '''select WholesaleHotelID from WholesaleHotel''', connAdaptor)

dataframeHotel = pd.DataFrame(SQL_Query, columns=['WholesaleHotelID'])
val1 = []
val2 = []

for label, row in dataframeHotel.iterrows():
    url = feed_url % (row["WholesaleHotelID"])
    var_url = urlopen(url)
    xmldoc = parse(var_url)

    for item in xmldoc.iterfind('.//roomtype'):
        hotel_id = item.findtext('hotel_id')
        hotel_room_type_id = item.findtext('hotel_room_type_id')
        size_of_room = item.findtext('size_of_room')
        no_of_room = item.findtext('no_of_room')
        views = item.findtext('views')
        bed_type = item.findtext('bed_type')
        hotel_room_type_picture = item.findtext('hotel_room_type_picture')

        print(hotel_id)
        print(hotel_room_type_id)
#        print(hotel_room_type_picture)
#        print(views)
        print()

        val1.append((
            hotel_room_type_id,
            hotel_id,
            size_of_room,
            no_of_room,
            views,
            bed_type,
            agoda_wholesale_id
        ))

        val2.append((
            hotel_room_type_id,
            agoda_wholesale_id,
            hotel_room_type_picture
        ))

crsr = connAdaptor.cursor()
crsr.fast_executemany = True

sql1 = "INSERT INTO WholesaleRoomType (WholesaleRoomTypeID, WholesaleHotelID, RoomSize, NumberOfRoom, RoomView, BedType, WholesaleID) " \
       "VALUES (?, ?, ?, ?, ?, ?, ?)"

sql2 = "INSERT INTO WholesaleRoomTypeImage (WholesaleRoomTypeID, WholesaleID, Image) " \
       "VALUES (?, ?, ?)"

crsr.executemany(sql1, val1)
connAdaptor.commit()

crsr.executemany(sql2, val2)
connAdaptor.commit()
crsr.close()
connAdaptor.close()

exit()
