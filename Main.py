import time

import mp as mp

from Country import Country
from CreateHotel import CreateHotel
from Destination import Destination
from Facility import Facility
from Hotel import Hotel
from RoomType import RoomType

if __name__ == '__main__':
    #print("Number of processors: ", mp.cpu_count())
    start_time = time.time()
    Country().run()
    Destination().run()
    Facility().run()

    Hotel().run()
    all_hotel_feed_df = RoomType().run()
    CreateHotel().run(all_hotel_feed_df)
    duration = time.time() - start_time
    print(f"Completed in {duration} seconds")
    print("Bye...")

    exit()

#   DELETE FROM [adaptor].[dbo].[WholesaleCountry]
#   DELETE FROM [act].[dbo].[Destination] WHERE CreatedAt > CONVERT(DATETIME, '2019-09-18')
#   DELETE FROM [adaptor].[dbo].[WholesaleDestination]
#   DELETE FROM [adaptor].[dbo].[WholesaleFacility]
#   DELETE FROM [adaptor].[dbo].[WholesaleHotel]
#   DELETE FROM [adaptor].[dbo].[WholesaleRoomType]
#   DELETE FROM [adaptor].[dbo].[WholesaleRoomTypeImage]
