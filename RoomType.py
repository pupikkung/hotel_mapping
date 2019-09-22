from typing import List, Any, Tuple

import numpy
import pyodbc
import pandas as pd
from urllib.request import urlopen
from xml.etree.ElementTree import parse

from ConnectorUtility import get_adaptor_connection


class RoomType:
    def run(self):
        agoda_wholesale_id = 2

        print("Welcome to the show!")
        connAdaptor = get_adaptor_connection()
        SQL_Query = pd.read_sql_query('''select top (1) WholesaleHotelID from WholesaleHotel''',
                                      connAdaptor)
        dataframeHotel = pd.DataFrame(SQL_Query, columns=['WholesaleHotelID'])

        all_hotel_feed_df = pd.DataFrame(
            {'hotel_id': [], 'remark': [], 'infant_age': [], 'children_age_from': [], 'children_age_to': [],
             'children_stay_free': [],
             'min_guest_age': []})

        wholesale_roomtype_array = []
        wholesale_roomtype_image_array = []

        for label, row in dataframeHotel.iterrows():
            print('WholesaleHotel Id %r' % row["WholesaleHotelID"])
            hotel_feed_df, wholesale_roomtype, wholesale_roomtype_image = self.get_feed(row["WholesaleHotelID"],
                                                                                        agoda_wholesale_id)
            all_hotel_feed_df = all_hotel_feed_df.append(hotel_feed_df, ignore_index=True)
            if not wholesale_roomtype_array:
                wholesale_roomtype_array = wholesale_roomtype
                # wholesale_roomtype_image_array = wholesale_roomtype_image
            elif len(wholesale_roomtype) > 0:
                numpy.append(wholesale_roomtype_array, wholesale_roomtype, axis=0)
                # numpy.append(wholesale_roomtype_image_array, wholesale_roomtype_image, axis=0)
            else:
                print('Content of hotel Id %r not found' % row["WholesaleHotelID"])

        self.insert_roomtype(wholesale_roomtype_array, connAdaptor)
        # self.insert_roomtype_image(wholesale_roomtype_image_array, connAdaptor)

        return all_hotel_feed_df

    def insert_roomtype(self, wholesale_roomtype_array, conn):
        print('Start insert_roomtype...')
        sql = "INSERT INTO WholesaleRoomType (WholesaleRoomTypeID, BedType, Image, NumberOfRoom, RoomSize, RoomView, WholesaleID, WholesaleHotelID) " \
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        cursorAdaptor = conn.cursor()
        cursorAdaptor.fast_executemany = True

        # cursorAdaptor.executemany(sql, wholesale_roomtype_array)
        conn.commit()
        cursorAdaptor.close()
        print('End insert_data')

    def insert_roomtype_image(self, wholesale_roomtype_image_array, conn):
        print('Start insert_roomtype...')
        sql = "INSERT INTO WholesaleRoomTypeImage (WholesaleRoomTypeID, WholesaleID, Image) " \
              "VALUES (?, ?, ?)"
        cursorAdaptor = conn.cursor()
        cursorAdaptor.fast_executemany = True

        # cursorAdaptor.executemany(sql, wholesale_roomtype_image_array)
        conn.commit()
        cursorAdaptor.close()
        conn.close()
        print('End insert_data')

    def get_feed(self, wholesaleHotelId, agoda_wholesale_id):
        feed_url = 'http://affiliatefeed.agoda.com/datafeeds/feed/getfeed?apikey=aaad14ef-e6d9-42ca-80ba-d73a7c82c33e&mhotel_id=%s&feed_id=19'
        url = feed_url % (wholesaleHotelId)
        var_url = urlopen(url)
        xmldoc = parse(var_url)

        hotel_feed_df = pd.DataFrame(
            {'hotel_id': [], 'remark': [], 'infant_age': [], 'children_age_from': [], 'children_age_to': [],
             'children_stay_free': [],
             'min_guest_age': []})
        for itemHotel in xmldoc.iterfind('.//hotel'):
            hotel_id = itemHotel.findtext('hotel_id')
            remark = itemHotel.findtext('remark')
            infant_age = itemHotel.findtext('.//infant_age')
            children_age_from = itemHotel.findtext('.//children_age_from')
            children_age_to = itemHotel.findtext('.//children_age_to')
            children_stay_free = itemHotel.findtext('.//children_stay_free')
            min_guest_age = itemHotel.findtext('.//min_guest_age')
            hotel_feed_df = hotel_feed_df.append(
                {'hotel_id': hotel_id, 'remark': remark, 'infant_age': infant_age,
                 'children_age_from': children_age_from,
                 'children_age_to': children_age_to, 'children_stay_free': children_stay_free,
                 'min_guest_age': min_guest_age}, ignore_index=True)

        wholesale_roomtype = []
        wholesale_roomtype_image = []
        for item in xmldoc.iterfind('.//roomtype'):
            hotel_room_type_id = item.findtext('hotel_room_type_id')
            bed_type = item.findtext('bed_type')
            hotel_room_type_picture = item.findtext('hotel_room_type_picture')
            no_of_room = item.findtext('no_of_room')
            size_of_room = item.findtext('size_of_room')
            views = item.findtext('views')
            hotel_id = item.findtext('hotel_id')

            wholesale_roomtype.append((
                hotel_room_type_id,
                bed_type,
                hotel_room_type_picture,
                no_of_room,
                size_of_room,
                views,
                agoda_wholesale_id,
                hotel_id
            ))

            wholesale_roomtype_image.append((
                hotel_room_type_id,
                agoda_wholesale_id,
                hotel_room_type_picture
            ))

        return hotel_feed_df, wholesale_roomtype, wholesale_roomtype_image
