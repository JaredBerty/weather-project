import re
import sqlite3
import urllib.error
import urllib.parse
import urllib.request


def create_table():
    """ Create sqlite table that will store station info """
    # connect to sqlite database
    conn = sqlite3.connect('update_station.sqlite')
    cur = conn.cursor()

    # Delete table and recreate each run, for troubleshooting
    cur.execute('''DROP TABLE IF EXISTS Station''')

    # Create Tables for locations and Station IDs
    cur.execute('''CREATE TABLE IF NOT EXISTS Station
        (id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, name TEXT, usaf_id TEXT, wban_id TEXT)''')
    print("Table created")


def pull_station_info():
    """ pull weather station list from https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt and
     put it into a dictionary"""

    counter = 0
    # saved file for testing, will have it use website when live to get new updates
    info = {}
    with urllib.request.urlopen('https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt') as fhand:
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


def populate_stations():
    info = pull_station_info()
    conn = sqlite3.connect('update_station.sqlite')
    cur = conn.cursor()

    for station_name, station_data in info.items():
        # get station data
        usaf_id = station_data['usaf_id']
        wban_id = station_data['wban_id']
        # Insert into database
        cur.execute('INSERT OR IGNORE INTO Station (name, usaf_id, wban_id) VALUES ( ?, ?, ? )',
                    (station_name, usaf_id, wban_id))

    conn.commit()
    conn.close()


def test():
    info = pull_station_info()
    for station, data in info.items():
        print(station)
        for key, value in data.items():
            print(key, value)

    print(info)


create_table()
populate_stations()
