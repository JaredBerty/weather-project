import json
import urllib.error, urllib.request
import sqlite3

station_id = "72466093037"


def clean_value(value):
    return value.replace('"', "").replace("\\", "").replace("\n", "").strip()


def compile_weather_data(station_id):
    file_name = f'csv_urls_{station_id}.json'
    # data = {"date": {}, "data": {}, "location_info": {}}
    data = {"location_info": {}, "dates": {}}
    with open(file_name, 'r') as f:
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

            line_values = [clean_value(value) for value in line_values]
            weather_data = [clean_value(value) for value in weather_data]

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

    file_name = f"data_for_{station_id}.json"
    with open(file_name, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"data saved for {file_name}")

    return data


def create_table(station_id):
    conn = sqlite3.connect('new_station_bulk.sqlite')
    cur = conn.cursor()
    table_name = "station" + station_id
    try:
        # Drop table if exists
        query = f'DROP TABLE IF EXISTS {table_name}'
        cur.execute(query)

        # Create new table
        query = f'''CREATE TABLE IF NOT EXISTS {table_name} (date DATETIME PRIMARY KEY UNIQUE, sta_ID INTEGER, 
        temp REAL, temp_atr INTEGER, dewp REAL, dewp_atr INTEGER, slp REAL, slp_atr INTEGER, stp REAL, stp_atr REAL, visib REAL, 
        visb_atr INTEGER, wind_speed REAL, wind_speed_atr INTEGER, max_speed REAL, gust REAL, max_val REAL,
         max_atr INTEGER, min_val REAL, min_atr INTEGER, prcp REAL, prcp_atr TEXT, sndp REAL, frshtt REAL)'''

        cur.execute(query)

        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print("Error creating table:", e)
    finally:
        conn.close()


def populate_bulk_table(station_id):
    table_name = "station" + station_id
    conn = sqlite3.connect('new_station_bulk.sqlite')
    cur = conn.cursor()
    # data = compile_weather_data(station_id)

    file_name = f'data_for_{station_id}.json'
    # data = {"date": {}, "data": {}, "location_info": {}}
    with open(file_name, 'r') as data:
        contents = json.load(data)

    for date, weather_data_for_date in contents["dates"].items():
        #query = f'INSERT OR IGNORE INTO "{table_name}" (date) VALUES (?)'
        #cur.execute(query, (date,))
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

            query = f'''INSERT OR IGNORE INTO "{table_name}" (date, temp, temp_atr, dewp, dewp_atr, slp, slp_atr, stp,
             stp_atr, visib, visb_atr, wind_speed, wind_speed_atr, max_speed, gust, max_val, max_atr, min_val, 
             min_atr, prcp, prcp_atr, sndp, frshtt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
            cur.execute(query, (date, temp, temp_atr, dewp, dewp_atr, slp, slp_atr, stp,
                                stp_atr, visib, visb_atr, wind_speed, wind_speed_atr, max_speed, gust, max_val, max_atr,
                                min_val,
                                min_atr, prcp, prcp_atr, sndp, frshtt))
            print(temp)

    conn.commit()
    conn.close()


create_table(station_id)
# compile_weather_data(station_id)
populate_bulk_table(station_id)
