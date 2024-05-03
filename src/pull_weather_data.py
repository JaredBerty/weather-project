import json
import urllib.error, urllib.request
import sqlite3
import os
import config
from directory_management import DirectoryManager


class PullWeatherData:

    def __init__(self, station_id):
        self.station_id = station_id
        self.station_data_table = f'station_data_{self.station_id}'
        self.station_list_table = 'station_list'
        self.db_name = config.DB_NAME
        self.weather_data = None

    def clean_value(self, value):
        return value.replace('"', "").replace("\\", "").replace("\n", "").strip()

    def compile_weather_data(self):

        DirectoryManager.json()
        try:

            file_name_csv = f'csv_urls_{self.station_id}.json'
            file_name_station_data = f"data_for_{self.station_id}.json"

            if self.weather_data is not None:
                print("data weather already initialized")
                return self.weather_data

            if os.path.exists(file_name_station_data):
                with open(file_name_station_data, 'r') as f:
                    self.weather_data = json.load(f)
                print("weather data already compiled")
                return self.weather_data
            # Parse data from list of CSVs
            data = {"location_info": {}, "dates": {}}
            with open(file_name_csv, 'r') as f:
                contents = json.load(f)

            for key in contents.keys():

                if contents[key] == "No Data": continue

                url = urllib.request.urlopen(contents[key])
                is_header = True

                for line in url:
                    line = line.decode().strip().split(',')

                    if is_header:
                        is_header = False
                        continue

                    line_values = line[:29]  # Extract the first 29 values from the line
                    sta_id, date, lat, long, elev, name, country, *weather_data = line_values

                    date = date.replace(' ', "").replace('"', "")

                    # Unpack the weather data into individual variables (if needed)
                    temp, temp_atr, dewp, dewp_atr, slp, slp_atr, stp, stp_atr, visib, visb_atr, \
                    wind_speed, wind_speed_atr, max_speed, gust, max_val, max_atr, \
                    min_val, min_atr, prcp, prcp_atr, sndp, frshtt = weather_data

                    line_values = [self.clean_value(value) for value in line_values]
                    weather_data = [self.clean_value(value) for value in weather_data]

                    if not data["location_info"]:
                        data["location_info"] = {
                            "sta_id": line_values[0],
                            "lat": line_values[2],
                            "long": line_values[3],
                            "elev": line_values[4],
                            "name": line_values[5],
                            "country": line_values[6]
                        }

                    weather_data_dict = {
                        "temp": weather_data[0],
                        "temp_atr": weather_data[1],
                        "dewp": weather_data[2],
                        "dewp_atr": weather_data[3],
                        "slp": weather_data[4],
                        "slp_atr": weather_data[5],
                        "stp": weather_data[6],
                        "stp_atr": weather_data[7],
                        "visib": weather_data[8],
                        "visb_atr": weather_data[9],
                        "wind_speed": weather_data[10],
                        "wind_speed_atr": weather_data[11],
                        "max_speed": weather_data[12],
                        "gust": weather_data[13],
                        "max_val": weather_data[14],
                        "max_atr": weather_data[15],
                        "min_val": weather_data[16],
                        "min_atr": weather_data[17],
                        "prcp": weather_data[18],
                        "prcp_atr": weather_data[19],
                        "sndp": weather_data[20],
                        "frshtt": weather_data[21]
                    }

                    if date not in data["dates"]:
                        data["dates"][date] = []
                        data["dates"][date].append(weather_data_dict)

            # save data to json so it's quicker to access
            with open(file_name_station_data, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"data saved for {file_name_station_data}")

            self.weather_data = data
            print("weather data compiled")
            return data
        finally:
            DirectoryManager.root()

    def create_table(self):

        DirectoryManager.root()
        try:
            with sqlite3.connect(self.db_name) as conn:
                cur = conn.cursor()
                # table_name = self.table_name
                try:
                    # Drop table if exists
                    query = f'DROP TABLE IF EXISTS {self.station_data_table}'
                    cur.execute(query)

                    # Create new table
                    query = f'''CREATE TABLE IF NOT EXISTS {self.station_data_table} (date DATETIME PRIMARY KEY UNIQUE, sta_ID INTEGER, 
                    temp REAL, temp_atr INTEGER, dewp REAL, dewp_atr INTEGER, slp REAL, slp_atr INTEGER, stp REAL, stp_atr REAL, visib REAL, 
                    visb_atr INTEGER, wind_speed REAL, wind_speed_atr INTEGER, max_speed REAL, gust REAL, max_val REAL,
                     max_atr INTEGER, min_val REAL, min_atr INTEGER, prcp REAL, prcp_atr TEXT, sndp REAL, frshtt REAL)'''

                    cur.execute(query)

                    print(f"Table '{self.station_data_table}' created successfully in {self.db_name}.")
                except Exception as e:
                    print(f"Error creating table: {self.station_data_table}", e)
                    raise
        finally:
            DirectoryManager.root()

    def populate_bulk_table(self):
        # table_name = "station_data_" + station_id
        DirectoryManager.root()
        try:
            with sqlite3.connect(self.db_name) as conn:
                cur = conn.cursor()
                self.compile_weather_data()

                DirectoryManager.json()
                file_name = f'data_for_{self.station_id}.json'
                # data = {"date": {}, "data": {}, "location_info": {}}

                with open(file_name, 'r') as data:
                    data = json.load(data)

                if not os.getcwd() == DirectoryManager.root():
                    DirectoryManager.root()
                    print("FARTS")

                for date, weather_data_for_date in data["dates"].items():

                    # query = f'INSERT OR IGNORE INTO "{table_name}" (date) VALUES (?)'
                    # cur.execute(query, (date,))
                    for weather_data in weather_data_for_date:
                        temp = weather_data.get("temp")
                        temp_atr = weather_data.get("temp_atr")
                        dewp = weather_data.get("dewp")
                        dewp_atr = weather_data.get("dewp_atr")
                        slp = weather_data.get("slp")
                        slp_atr = weather_data.get("slp_atr")
                        stp = weather_data.get("stp")
                        stp_atr = weather_data.get("stp_atr")
                        visib = weather_data.get("visib")
                        visb_atr = weather_data.get("visb_atr")
                        wind_speed = weather_data.get("wind_speed")
                        wind_speed_atr = weather_data.get("wind_speed_atr")
                        max_speed = weather_data.get("max_speed")
                        gust = weather_data.get("gust")
                        max_val = weather_data.get("max_val")
                        max_atr = weather_data.get("max_atr")
                        min_val = weather_data.get("min_val")
                        min_atr = weather_data.get("min_atr")
                        prcp = weather_data.get("prcp")
                        prcp_atr = weather_data.get("prcp_atr")
                        sndp = weather_data.get("sndp")
                        frshtt = weather_data.get("frshtt")


                        query = f'''INSERT OR IGNORE INTO "{self.station_data_table}" (date, temp, temp_atr, dewp, dewp_atr, slp, slp_atr, stp,
                         stp_atr, visib, visb_atr, wind_speed, wind_speed_atr, max_speed, gust, max_val, max_atr, min_val, 
                         min_atr, prcp, prcp_atr, sndp, frshtt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
                        cur.execute(query, (date, temp, temp_atr, dewp, dewp_atr, slp, slp_atr, stp,
                                            stp_atr, visib, visb_atr, wind_speed, wind_speed_atr, max_speed, gust,
                                            max_val, max_atr, min_val, min_atr, prcp, prcp_atr, sndp, frshtt))
                        # print(temp)

                conn.commit()

            # Get number of rows in table
            query = f'''SELECT max(rowid) FROM {self.station_data_table}'''
            cur.execute(query)
            n = cur.fetchone()[0]

            print(f"{self.station_data_table} was populated with {n} rows of weather data in {self.db_name}.")
        finally:
            DirectoryManager.root()

    def associate_data_to_station_list(self):
        DirectoryManager.root()
        with sqlite3.connect(self.db_name) as conn:
            cur = conn.cursor()

            query = f'''SELECT * FROM {self.station_list_table} WHERE usaf_id || wban_id=?'''
            cur.execute(query, (self.station_id,))
            n = cur.fetchone()
            if n:
                query = f'''UPDATE {self.station_data_table} SET sta_ID=?'''
                cur.execute(query, (n[0],))
                print(f'Found a match ({n[0:4]}). updated sta_ID in {self.station_data_table}')

            conn.commit()

    def update_location_info_in_station_list(self):
        DirectoryManager.root()

        location_info = self.weather_data.get("location_info", {})
        lat = location_info.get('lat')
        long = location_info.get('long')
        elev = location_info.get('elev')
        name = location_info.get('name')
        country = location_info.get('country')
        # print(lat, long, elev, name, country)

        # connect to database
        with sqlite3.connect(self.db_name) as conn:
            cur = conn.cursor()

            # fetch corresponding row in the staion_list table
            query = f'''SELECT * FROM {self.station_list_table} WHERE usaf_id || wban_id=?'''
            cur.execute(query, (self.station_id,))

            result = cur.fetchone()
            if result:
                sta_id = result[0]
                query = f'UPDATE {self.station_list_table} SET lat=?, long=?, elev=?, country=? WHERE id=?'
                cur.execute(query, (lat, long, elev, country, sta_id,))
                print(f"{self.station_list_table} succesfully updated station {self.station_id} location info.")
            else:
                print(f"No matching record found in the {self.station_list_table} table.")


colorado_weather = PullWeatherData(config.TEST_STATION_ID)
colorado_weather.compile_weather_data()
colorado_weather.create_table()
colorado_weather.populate_bulk_table()
colorado_weather.associate_data_to_station_list()
colorado_weather.update_location_info_in_station_list()
