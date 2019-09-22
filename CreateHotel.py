import pandas as pd
import pyodbc

from ConnectorUtility import get_adaptor_connection, get_act_connection


class CreateHotel:
    def run(self, all_hotel_feed_df: pd.DataFrame):
        hotel_mapping_result_path = 'resources/Hotel Mapping Thai only.xlsx'
        agoda_wholesale_id = 2
        htb_wholesale_id = 1

        print("Welcome to the show!")

        connAdaptor = get_adaptor_connection()
        connAct = get_act_connection()

        dataframeMappingHotelFile = pd.read_excel(hotel_mapping_result_path, sheet_name='Hotel List')

        print("found mapping %r " % dataframeMappingHotelFile.shape[0])
        dataframeMappingHotelForInsert = dataframeMappingHotelFile[
            pd.notnull(dataframeMappingHotelFile['AgodaHotelID'])]
        dataframeMappingHotelForInsert = dataframeMappingHotelForInsert[pd.isnull(dataframeMappingHotelForInsert['ACTHotelID'])]
        dataframeMappingHotelForInsert = dataframeMappingHotelForInsert[pd.isnull(dataframeMappingHotelForInsert['HTBHotelID'])]
        print('Number of hotel need to be inserted %r' % dataframeMappingHotelForInsert.shape[0])

        dataframeMappingHotelMatchBoth = dataframeMappingHotelFile[
            pd.notnull(dataframeMappingHotelFile['AgodaHotelID'])]
        dataframeMappingHotelMatchBoth = dataframeMappingHotelMatchBoth[pd.notnull(dataframeMappingHotelMatchBoth['ACTHotelID'])]
        dataframeMappingHotelMatchBoth = dataframeMappingHotelMatchBoth[pd.notnull(dataframeMappingHotelMatchBoth['HTBHotelID'])]
        print('Number of hotel exist both supplier %r' % dataframeMappingHotelMatchBoth.shape[0])

        dataframeMappingHotelMatchActAgoda = dataframeMappingHotelFile[
            pd.notnull(dataframeMappingHotelFile['AgodaHotelID'])]
        dataframeMappingHotelMatchActAgoda = dataframeMappingHotelMatchActAgoda[pd.notnull(dataframeMappingHotelMatchActAgoda['ACTHotelID'])]
        dataframeMappingHotelMatchActAgoda = dataframeMappingHotelMatchActAgoda[
            pd.isnull(dataframeMappingHotelMatchActAgoda['HTBHotelID'])]
        print('Number of hotel exist only Agoda supplier %r' % dataframeMappingHotelMatchActAgoda.shape[0])

        dataframeMappingHotelMatchActHTB = dataframeMappingHotelFile[
            pd.notnull(dataframeMappingHotelFile['HTBHotelID'])]
        dataframeMappingHotelMatchActHTB = dataframeMappingHotelMatchActHTB[
            pd.notnull(dataframeMappingHotelMatchActHTB['ACTHotelID'])]
        dataframeMappingHotelMatchActHTB = dataframeMappingHotelMatchActHTB[
            pd.isnull(dataframeMappingHotelMatchActHTB['AgodaHotelID'])]
        print('Number of hotel exist only HTB supplier %r' % dataframeMappingHotelMatchActHTB.shape[0])

        sql = "SELECT WholesaleID, NameEN, Priority, SupplierID FROM Wholesale"
        wholesaleDf = pd.read_sql_query(sql, connAdaptor)

        htb_priority = int(wholesaleDf.loc[wholesaleDf['NameEN'] == 'Hotelbeds'].Priority)
        agoda_priority = int(wholesaleDf.loc[wholesaleDf['NameEN'] == 'Agoda'].Priority)
        print(agoda_priority > htb_priority)

    def not_exist_case(self, dataframeMappingHotelForInsert):
        print('Start not_exist_case...')
        # TODO insert_act_hotel
        # TODO insert_act_hotel_supplier

    def exist_act_case(self, dataframeMappingHotelMatchActAgoda):
        print('Start exist_act_case...')
        # TODO insert_act_hotel_supplier

    def exist_both_supplier_case(self, dataframeMappingHotelMatchBoth):
        print('Start exist_both_supplier_case...')
        # TODO check_priority_and_update_if_need
        # TODO insert_act_hotel_supplier

    def insert_act_hotel(self, hotel_array, conn):
        print('Start insert_act_hotel...')
        sql = "INSERT INTO Hotel (HotelID, DestinationID, NameTH, NameEN, AddressTH, AddressEN" \
              "DetailTH, DetailEN, Slug, Latitude, Longitude, ContactNumber, HotelStar," \
              "PointPriority, TimeServiceBegin, TimeServiceEnd, AllotmentControl, Keyword," \
              "Status, Active, CreatedAt, UpdatedAt, LatestRate, AccommodationCategoryID, RemarkEN, RemarkTH ) " \
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?)"
        cursorAct = conn.cursor()
        cursorAct.fast_executemany = True

        cursorAct.executemany(sql, hotel_array)
        conn.commit()
        cursorAct.close()
        print('End insert_data')

    def insert_act_hotel_age_policy(self, hotel_age_policy_array, conn):
        print('Start insert_act_hotel_age_policy...')
        sql = "INSERT INTO HotelAgePolicy (HotelID, MinimumAge, InfantAge, ChildrenAgeFrom, ChildrenAgeTo, Status)" \
              "VALUES (?, ?, ?, ?, ?, ?)"
        cursorAct = conn.cursor()
        cursorAct.fast_executemany = True

        cursorAct.executemany(sql, hotel_age_policy_array)
        conn.commit()
        cursorAct.close()
        print('End insert_data')

    def insert_act_hotel_recreation_facility(self, hotel_recreation_facility_array, conn):
        print('Start insert_act_hotel_recreation_facility...')
        sql = "INSERT INTO HotelRecreationFacility (HotelID, RecreationFacilityID)" \
              "VALUES (?, ?)"
        cursorAct = conn.cursor()
        cursorAct.fast_executemany = True

        cursorAct.executemany(sql, hotel_recreation_facility_array)
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


