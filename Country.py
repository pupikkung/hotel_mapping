import pandas as pd

from ConnectorUtility import get_adaptor_connection


class Country:
    def run(self):
        # File configuration
        country_mapping_result_path = 'resources/country_mapping_final_data.xlsx'
        agoda_wholesale_id = 2

        print("Welcome to the show!")
        dataframeMappingCountry = pd.read_excel(country_mapping_result_path, sheet_name='in')

        sql = "INSERT INTO WholesaleCountry (WholesaleCountryID, CountryID, ISO2, ISO3, Latitude, Longtitude, NameEN, WholesaleID) " \
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

        print("found %r records " % dataframeMappingCountry.shape[0])
        # Drop rows with any empty cells
        dataframeMappingCountry = dataframeMappingCountry[pd.notnull(dataframeMappingCountry['AgodaCountryID'])]
        print("after drop null, have %r records " % dataframeMappingCountry.shape[0])

        val = []
        for label, row in dataframeMappingCountry.iterrows():
            val.append((
                row["AgodaCountryID"],
                row["ACTCountryID"],
                row["AgodaISO2"],
                row["AgodaISO3"],
                row["AgodaLatitude"],
                row["AgodaLongtitude"],
                row["AgodaNameEN"],
                agoda_wholesale_id
            ))

        connAdaptor = get_adaptor_connection()
        cursorAdaptor = connAdaptor.cursor()
        cursorAdaptor.fast_executemany = True

        cursorAdaptor.executemany(sql, val)
        connAdaptor.commit()
        cursorAdaptor.close()
        connAdaptor.close()
