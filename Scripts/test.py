import json

station_id = "72466093037"

file_name = f'data_for_{station_id}.json'
data = {"date": {}, "data": {}, "location_info": {}}
with open(file_name, 'r') as f:
    contents = json.load(f)

    for key,value in contents.items():
        for date in value.items():
            print(date)