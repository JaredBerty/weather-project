import re
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
import config


class UpdateStationList:

    def __init__(self):
        self.db_name = config.DB_NAME
        self.station_list_url = config.STATION_LIST_URL
        self.table_name = 'station_list'

    def create_table(self):
        """ Create sqlite table that will store station info """
        # connect to sqlite database
        with sqlite3.connect(self.db_name) as conn:
            cur = conn.cursor()

            # ask user if they want to reset the station list table to empty

            cur.execute('''DROP TABLE IF EXISTS station_list''')

            # Create Tables for locations and Station IDs
            cur.execute('''CREATE TABLE IF NOT EXISTS station_list
                (id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, name TEXT, usaf_id TEXT, wban_id TEXT, lat REAL,
                long REAL, elev REAL, country TEXT)''')

            print(f"Table 'station_list' created on {self.db_name}.")

    def _pull_station_info(self):
        """ pull weather station list from https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt and
         put it into a dictionary"""

        counter = 0
        # saved file for testing, will have it use website when live to get new updates
        info = {}
        with urllib.request.urlopen(self.station_list_url) as fhand:
            # with open("isd-history.txt") as fhand:
            for line in fhand:
                line = line.decode().strip()
                # line = line.strip()
                station_info = line[:43]
                location_info = line[43:]

                # skip lines not starting with valid station ID
                if not re.match(r'^[A-Z0-9]{6}', line):
                    continue
                # skip stations without names
                if not re.match(r'\b[A-Z0-9]{6} \d{5}.*[A-Z0-9]', station_info):
                    continue

                # extract station name, USAF ID, and WBAN_ID
                station_name = re.findall(r'[A-Z0-9]{6} \d{5} ([\w\s/.-]+)', station_info)[0].strip()
                usaf_id, wban_id = station_info.split()[:2]

                # construct a dictionary to hold the station data
                # need to add fields for the rest of the data parts still
                station_data = {'usaf_id': usaf_id, 'wban_id': wban_id}

                # add station data to dictionary
                info[station_name] = station_data

                # break after going through a certain number of entries (for testing)
                # counter += 1
                # if counter == 30: break

        return info

    def populate_stations(self):
        info = self._pull_station_info()
        with sqlite3.connect(self.db_name) as conn:
            cur = conn.cursor()

            for station_name, station_data in info.items():
                # get station data
                usaf_id = station_data['usaf_id']
                wban_id = station_data['wban_id']
                # Insert into database
                cur.execute('INSERT OR IGNORE INTO station_list (name, usaf_id, wban_id) VALUES ( ?, ?, ? )',
                            (station_name, usaf_id, wban_id))

            conn.commit()

        print(f"Station list populated to {self.db_name} successfully.")

    def test(self):
        info = self._pull_station_info()
        for station, data in info.items():
            print(station)
            for key, value in data.items():
                print(key, value)

        print(info)


update = UpdateStationList()
update.create_table()
update.populate_stations()