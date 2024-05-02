import sqlite3
import config
import re
from datetime import datetime

class WeatherPredictor:
    """
    A Class that will predict various weather attributes for a date base on user input.

    Attributes:
        db_name (str): the name of the sqlite database
        station_id (str): The ID of the weather station
        station_data_table (str): The name of the table containing station data
        date (str): The selected date for the weather prediction
    """

    def __init__(self):
        """
        Initializes  a WeatherPredictor object.

        This method sets up the initial state of the WeatherPredictor object by initializing attributes
        db_name, station_id, station_data_table and the date.

        config has the database name saved as DB_NAME, as well as TEST_STATION_ID which is used for testing the
        different methods and classes for the project.
        """
        self.db_name = config.DB_NAME
        self.station_id = config.TEST_STATION_ID
        self.station_data_table = f'station_data_{self.station_id}'
        self.date = self.date_request()

    def date_request(self):
        """
        Requests a valid date input from the user.

        This method prompts the user to input a date in the MM-DD format and validates the input to ensure its a valid
        date format. It loops until a valid date is provided.

        Returns:
            str: A string representing the selected date in the MM-DD format
        """
        calendar = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

        loop = True
        counter = 0
        while loop:
            if counter >= 3:
                user_help = input("\n do you need an example or something? input 'Y' for remedial training: ")
                if user_help.lower() == 'y':
                    print("09-15 represents september 15th.")

            request = input("Please input a date in format MM-DD, or 'Q' to quit: ")
            # check that request is in MM-DD format
            if request.lower() == 'q':
                print('quitting')
                break
            if re.match(r'[0-9]{2}-[0-9]{2}', request):
                pass
            else:
                counter = counter + 1
                print(f'{request} is not a valid input, please use MM-DD format')
                continue

            # check that request is a valid month and date
            check = request.split("-")
            MM = int(check[0])
            DD = int(check[1])

            try:
                if DD <= calendar[MM]:
                    print('valid month and date')
                    loop = False
                else:
                    print(f'{MM}-{DD} not a valid date in the calender')
                    counter = counter + 1
            except:
                print(f'{MM}-{DD} not a valid date in the calender')
                counter = counter + 1

        return request

    def make_temp_prediction(self):

        """
        Makes a temperature prediction based on the selected date.

        This method will fetch all historical temperature data for the selected date.
        """

        date = self.date
        print(type(date))
        with sqlite3.connect(self.db_name) as conn:
            cur = conn.cursor()

            query = f'''SELECT * FROM {self.station_data_table} WHERE substr(date,5,6)=?'''
            cur.execute(query, (date,))

            rows = cur.fetchone()

            print(rows)


new_prediction = WeatherPredictor()
new_prediction.make_temp_prediction()
