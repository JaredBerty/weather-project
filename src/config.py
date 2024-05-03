import os

DB_NAME = 'weather_channel.sqlite'
TEST_STATION_ID = "72466093037"  # Only used for troubleshooting, eventually the user should be able to input an ID.
STATION_LIST_URL = 'https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt'
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_DATA_DIR = os.path.join(ROOT_DIR, 'data', 'json')
