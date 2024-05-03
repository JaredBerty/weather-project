import re
import config
import urllib.error, urllib.request
from bs4 import BeautifulSoup
import json


# chatGPT helped clean up old code
class UrlScraper:

    def __init__(self):
        self.base_url = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/'

    def get_year_urls(self):
        url = urllib.request.urlopen(self.base_url)
        soup = BeautifulSoup(url, "html.parser")
        year_urls = [self.base_url + link['href'] for link in soup.find_all('a', href=re.compile(r'^\d{4}'))]
        return year_urls

    def extract_csv_urls(self, year_url, station_id):
        csv_urls = {}
        try:
            year_page = urllib.request.urlopen(year_url)
            soup = BeautifulSoup(year_page, "html.parser")
            for link in soup.find_all('a', href=re.compile(r'^\d{11}')):
                href = link['href']
                if station_id in href:
                    csv_urls[year_url[64:68]] = year_url + href
            if not csv_urls:
                csv_urls[year_url[64:68]] = "No Data"
        except Exception as e:
            print(f"Error fetching data for year {year_url[-4:]}:", e)
            csv_urls[year_url[64:68]] = "Error"
        return csv_urls

    def save_csv_urls(self, csv_urls, station_id):
        filename = f'csv_urls_{station_id}.json'
        with open(filename, 'w') as f:
            json.dump(csv_urls, f, indent=4)
        print(f"URLs saved to {filename}")

    def scrape_urls(self, station_id):
        year_urls = self.get_year_urls()
        csv_urls = {}
        for year_url in year_urls[:]:  # Limit amount of returns by year_urls[:20]
            csv_urls.update(self.extract_csv_urls(year_url, station_id))
        self.save_csv_urls(csv_urls, station_id)


scraper = UrlScraper()
scraper.scrape_urls(config.TEST_STATION_ID)
