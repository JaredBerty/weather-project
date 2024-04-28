import re
import sqlite3
from urllib.parse import urlparse
import urllib.error, urllib.request
from bs4 import BeautifulSoup
import json
import csv


def create_table(table_name):
    conn = sqlite3.connect('new_station_bulk.sqlite')
    cur = conn.cursor()

    table_name = table_name
    query = 'DROP TABLE IF EXISTS {}'.format(table_name)
    cur.execute(query)

    # Create table for station ID
    query = 'CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY UNIQUE, url TEXT'.format(table_name)
    cur.execute(query)


def get_urls(station_id):
    csv_urls = {}
    urls = []

    url = urllib.request.urlopen('https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/')
    soup = BeautifulSoup(url, "html.parser")

    # only get links to 'years' data
    for link in soup.find_all('a'):
        if not re.match('[0-9]{4}', link.get('href')[:4]): continue
        year = re.findall('[0-9]{4}', link.get('href')[:4])[0]
        href = link.get('href')
        urls.append(url + href)

    station_id = str(station_id)
    counter = 0
    for url in urls:
        counter = counter + 1
        if counter < 20:
            year = urllib.request.urlopen(url)
            soup = BeautifulSoup(year, "html.parser")
            data_found = False
            for link in soup.find_all('a'):
                if not re.match('[0-9]{11}', link.get('href')): continue
                href = link['href']

                if station_id not in href: continue
                if station_id in href:
                    csv_urls[url[64:68]] = (url + href)
                    data_found = True
            if not data_found:
                csv_urls[url[64:68]] = "No Data"
        else:
            break

    with open(f'csv_urls_{station_id}', 'w') as f:

        json.dump(csv_urls, f, indent=4)
        print("Urls saved")


def get_all_station_data(station_id):
    pass