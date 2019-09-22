import time


from ConnectorUtility import get_adaptor_connection, get_act_connection
from Country import Country
from CreateHotel import CreateHotel
from Destination import Destination
from Facility import Facility
from Hotel import Hotel
from RoomType import RoomType

if __name__ == '__main__':
    start_time = time.time()
    Country().run()
    # Destination().run()
    # Facility().run()

    # Hotel().run()
    all_hotel_feed_df = RoomType().run()
    CreateHotel().run(all_hotel_feed_df)
    duration = time.time() - start_time
    print(f"Completed in {duration} seconds")
    print("Bye...")

    exit()

def clear_db():
    connAdaptor = get_adaptor_connection()
    sql_Delete_query = 'DELETE FROM [adaptor].[dbo].[WholesaleRoomType]'
    cursor = connAdaptor.cursor()
    cursor.execute(sql_Delete_query)
    cursor.close()

    sql_Delete_query = 'DELETE FROM [adaptor].[dbo].[WholesaleHotel]'
    cursor = connAdaptor.cursor()
    cursor.execute(sql_Delete_query)
    cursor.close()

    sql_Delete_query = 'DELETE FROM [adaptor].[dbo].[WholesaleFacility]'
    cursor = connAdaptor.cursor()
    cursor.execute(sql_Delete_query)
    cursor.close()

    sql_Delete_query = 'DELETE FROM [adaptor].[dbo].[WholesaleDestination]'
    cursor = connAdaptor.cursor()
    cursor.execute(sql_Delete_query)
    cursor.close()

    sql_Delete_query = 'DELETE FROM [adaptor].[dbo].[WholesaleCountry]'
    cursor = connAdaptor.cursor()
    cursor.execute(sql_Delete_query)
    cursor.close()

    connAdaptor.commit()

    connAct = get_act_connection()
    sql_Delete_query = 'DELETE FROM [act].[dbo].[Destination] WHERE CreatedAt > CONVERT(DATETIME, "2019-09-22")'
    cursor = connAct.cursor()
    cursor.execute(sql_Delete_query)
    cursor.close()

    sql_Delete_query = 'DELETE FROM [act].[dbo].[HotelSupplier] WHERE SupplierID = 5159 AND PercentMarkup = 7.00'
    cursor = connAct.cursor()
    cursor.execute(sql_Delete_query)
    cursor.close()

    connAct.commit()

#   DELETE FROM [adaptor].[dbo].[WholesaleCountry]
#   DELETE FROM [act].[dbo].[Destination] WHERE CreatedAt > CONVERT(DATETIME, '2019-09-18')
#   DELETE FROM [act].[dbo].[HotelSupplier] WHERE SupplierID = 5159 AND PercentMarkup = 7.00
#   DELETE FROM [adaptor].[dbo].[WholesaleDestination]
#   DELETE FROM [adaptor].[dbo].[WholesaleFacility]
#   DELETE FROM [adaptor].[dbo].[WholesaleHotel]
#   DELETE FROM [adaptor].[dbo].[WholesaleRoomType]
#   DELETE FROM [adaptor].[dbo].[WholesaleRoomTypeImage]
