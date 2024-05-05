import sqlite3
import statistics

import config
import re
from directory_management import DirectoryManager
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
    calendar = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

    def __init__(self):
        """
        Initializes  a WeatherPredictor object.

        This method sets up the initial state of the WeatherPredictor object by initializing attributes
        db_name, station_id, station_data_table and the date.

        config has the database name saved as DB_NAME, as well as TEST_STATION_ID which is used for testing the
        different methods and classes for the project.
        """
        DirectoryManager.root()
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
        # calendar = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

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
                if DD <= WeatherPredictor.calendar[MM]:
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

        requested_date = self.date
        with sqlite3.connect(self.db_name) as conn:
            cur = conn.cursor()

            query = f'''SELECT * FROM {self.station_data_table} WHERE strftime('%m-%d', date)=?'''
            cur.execute(query, (requested_date,))

            rows = cur.fetchall()
            temp_list = []
            for row in rows:
                temp_list.append(row[2])
                print(row[0], row[2])
            # print(temp_list)
            print(statistics.fmean(temp_list))
            print(max(temp_list))
            print(min(temp_list))

    def weighted_average(self):

        # config
        weight_period = 14  # total window of time i want to look at (7 days back, 7 days ahead)
        calendar = WeatherPredictor.calendar
        date = self.date

        # logic
        split = date.split("-")
        MM = int(split[0])
        DD = int(split[1])

        date_range = []
        counter = 0
        # Need to make a loop that will append date_range with previous 6 dates, selected date, then the future 6 dates
        while counter <= weight_period:
            counter = counter + 1

            if counter <= 7:
                print("past", counter)
                if counter > DD:
                    substract_date = counter - DD
                    MM = MM - 1
                    DD = calendar[MM] - substract_date

                    previous_date = str(MM) + "-" + str(DD)
                    date_range.append(previous_date)

                    # print(weight_period, DD, weight_period - DD)
                elif counter < DD:
                    DD = DD - counter

                    previous_date = str(MM) + "-" + str(DD)
                    date_range.append(previous_date)

            if counter == 7:
                date_range.append(self.date)

            if counter > 7:
                if weight_period < DD:
                    add_date = DD - counter
                    if MM == 12:
                        MM = 1
                    else:
                        MM = MM + 1
                    DD = calendar[MM] + add_date

                    future_date = str(MM) + "-" + str(DD)
                    date_range.append(future_date)
                elif weight_period > DD:
                    DD = counter + DD

                    future_date = str(MM) + "-" + str(DD)
                    date_range.append(future_date)

        print(date_range)

        # collecting previous 6 dates

        '''
        # this didn't do what i needed, but i might use the logic
                if weight_period > DD:
            substract_date = weight_period - DD
            MM = MM - 1
            DD = calendar[MM] - substract_date

            previous_date = f"{MM}-{DD}"
            print(previous_date)

            # print(weight_period, DD, weight_period - DD)
        elif weight_period < DD:
            DD = DD - weight_period

            previous_date = f"{MM}-{DD}"
            print(previous_date)

        # collecting preceding 6 dates
        if weight_period < DD:
            add_date = DD - weight_period
            MM = MM + 1
            DD = calendar[MM] + add_date

            future_date = str(MM) + "-" + str(DD)
            print(future_date)
        elif weight_period > DD:
            DD = weight_period + DD

            future_date = str(MM) + "-" + str(DD)
            print(future_date)
        '''


new_prediction = WeatherPredictor()
# new_prediction.make_temp_prediction()
new_prediction.weighted_average()
