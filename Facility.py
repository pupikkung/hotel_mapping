import pandas as pd

from ConnectorUtility import get_adaptor_connection


class Facility:
    def run(self):
        # File configuration
        facility_mapping_result_path = 'resources/Astra Hotel Facilities Group.xlsx'
        agoda_wholesale_id = 2

        print("Welcome to the show!")
        dataframeMappingFacility = pd.read_excel(facility_mapping_result_path, sheet_name='ACT Facilities')

        print("found mapping %r " % dataframeMappingFacility.shape[0])
        dataframeMappingFacility = dataframeMappingFacility.dropna()
        dataframeMappingFacility.drop_duplicates(subset='Agoda ID', keep='last', inplace=True)
        print("exist in act  %r " % dataframeMappingFacility.shape[0])

        print('Start insert_data...')
        val = []
        for label, row in dataframeMappingFacility.iterrows():
            val.append((
                int(row["Agoda ID"]),
                int(row["ACT ID"]),
                row["Title EN"],
                agoda_wholesale_id
            ))
        sql = "INSERT INTO WholesaleFacility (WholesaleFacilityID, RecreationFacilityID, NameEN, WholesaleID) " \
              "VALUES (?, ?, ?, ?)"

        connAdaptor = get_adaptor_connection()
        cursorAdaptor = connAdaptor.cursor()
        cursorAdaptor.fast_executemany = True

        cursorAdaptor.executemany(sql, val)
        connAdaptor.commit()
        cursorAdaptor.close()
        connAdaptor.close()
        print('End insert_data')
