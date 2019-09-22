import pandas as pd
import pyodbc

from ConnectorUtility import get_adaptor_connection, get_act_connection


class CreateHotel:
    def run(self, all_hotel_feed_df: pd.DataFrame):
        agoda_hotel_result_path = 'resources/agoda_hotel_full_detail_en.csv'
        agoda_wholesale_id = 2

        print("Welcome to the show!")

        connAdaptor = get_adaptor_connection()
        connAct = get_act_connection()

        selectActJoinAdaptorHotelSql = "SELECT h.HotelID, h.DestinationID, h.NameTH, h.NameEN, h.AddressTH, h.AddressEN," \
                            "h.DetailTH, h.DetailEN, Slug, h.Latitude, Longitude, ContactNumber, HotelStar," \
                            "PointPriority, TimeServiceBegin, TimeServiceEnd, AllotmentControl, Keyword," \
                            "Status, Active, CreatedAt, UpdatedAt, LatestRate, AccommodationCategoryID," \
                            "w.WholesaleHotelID ,w.WholesaleDestinationID, w.WholesaleID " \
                            "FROM [act].[dbo].[Hotel] h " \
                            "JOIN [adaptor].[dbo].[WholesaleHotel] w " \
                            "ON h.HotelID = w.HotelID"

        existingHotelDf = pd.read_sql_query(selectActJoinAdaptorHotelSql, connAct)
        print(existingHotelDf)

        dataframeAgodaHotel = pd.read_csv(agoda_hotel_result_path, encoding='utf-8', low_memory=False)
        print(dataframeAgodaHotel)

        tempdf = dataframeAgodaHotel['hotel_id'].map(
            existingHotelDf.set_index(['WholesaleHotelID'])['HotelID'])

        print(tempdf)

        dataframeAgodaHotel = dataframeAgodaHotel.join(tempdf, how='left', lsuffix='_left', rsuffix='_right')
        print(dataframeAgodaHotel)

        updateHotelDf = dataframeAgodaHotel.loc[dataframeAgodaHotel['hotel_id_right'].notnull()]
        print('Number of hotel need to be updated %r' % updateHotelDf.shape[0])

        insertHotelDf = dataframeAgodaHotel.loc[dataframeAgodaHotel['hotel_id_right'].isnull()]
        print('Number of hotel need to be inserted %r' % insertHotelDf.shape[0])

        # Begin update hotel content
        # insert hotel supplier
        val = []
        for label, row in updateHotelDf.iterrows():
            val.append((
                5159, # Agoda supplierId
                row["hotel_id_right"],
                7.00,  # PercentMarkup
                1  # Active
            ))

        self.insert_act_hotel_supplier(val, connAct)


    def insert_act_hotel(self, hotel_array, conn):
        print('Start insert_act_hotel...')
        sql = "INSERT INTO Hotel (HotelID, DestinationID, NameTH, NameEN, AddressTH, AddressEN" \
              "DetailTH, DetailEN, Slug, Latitude, Longitude, ContactNumber, HotelStar," \
              "PointPriority, TimeServiceBegin, TimeServiceEnd, AllotmentControl, Keyword," \
              "Status, Active, CreatedAt, UpdatedAt, LatestRate, AccommodationCategoryID ) " \
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?, ?, ?, ?, ?, ?)"
        cursorAct = conn.cursor()
        cursorAct.fast_executemany = True

        cursorAct.executemany(sql, hotel_array)
        conn.commit()
        cursorAct.close()
        print('End insert_data')

    def insert_act_hotel_supplier(self, hotel_supplier_array, conn):
        print('Start insert_act_hotel_supplier...')
        sql = "INSERT INTO HotelSupplier (SupplierID, HotelID, PercentMarkup, Status)" \
              "VALUES (?, ?, ?, ?)"
        cursorAct = conn.cursor()
        cursorAct.fast_executemany = True

        cursorAct.executemany(sql, hotel_supplier_array)
        conn.commit()
        cursorAct.close()
        print('End insert_data')


