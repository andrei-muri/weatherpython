import requests
from connection import dbaccess
import os
import datetime
import json
import argparse

def fetch_data(location: str, hours):
    datetime_in_5h = datetime.datetime.now() + datetime.timedelta(hours=hours)

    # Format the datetime as ISO 8601 without milliseconds
    datetime_in_5h_str = datetime_in_5h.strftime('%Y-%m-%dT%H:%M:%S')
    params = {
        "key": os.getenv('WEATHER_API_KEY'),
        "unitGroup": "metric",
        "include": "current" 
    }
    response = requests.get(
    f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{datetime_in_5h_str}",
    params=params)

    if response.status_code != 200:
        print(f"Error in request: {response.status_code}")
        # exit()
    data = json.loads(response.text)
    with open('results.json', 'w') as f:
        json.dump(data, f, indent=4)
    

def main(location, hours):
    
    fetch_data(location, hours)
    with open('results.json', 'r') as f:
        data = json.load(f)
    try:
        with dbaccess.Connection("/home/muri/Desktop/some_python/weatherapiproject/databases/conditions.db") as conn:
            conn.add_location(location)
            datetime = "T".join([data['days'][0]['datetime'], data['currentConditions']['datetime']])
            conn.add_conditions(location, int(data['days'][0]['temp']), datetime)
            conn.see_all_conditions()
    except Exception as e:
        print(str(e))
        



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get weather conditions for location in n hours")
    
    parser.add_argument('location', type=str, help="Location for weather forecast")
    parser.add_argument('hours', type=int, help="Weather after how many hours after the current time")
    args = parser.parse_args()
    main(args.location, args.hours)