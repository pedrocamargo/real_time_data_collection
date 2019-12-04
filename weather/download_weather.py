import sys

# We try to install requests and pandas because the default Python 3.6 docker images do not have these packages
# If you deploy your own images, then you don't need these, but if you don;t, you definitely do. No harm in keeping it
try:
    import pip

    for p in ['requests', 'pandas', 'yaml']:
        pip.main(['install', p])
except:
    print("pip not available")

import requests
from time import sleep, time
from datetime import datetime, date
import sqlite3
import pandas as pd
import collections
import yaml
import os
import csv
import json


def main(apikey, interval):
    if interval <=1:
        interval = 1

    dir_path = os.path.dirname(os.path.realpath(__file__))

    reference = {"query_time": "123",
                 "country": "AU",
                 "place_name": "Brisbane",
                 "zipcode": "4000",
                 "coord": {"lon": 153.03,
                           "lat": -27.47},
                 "weather": {"id": 800,
                              "main": "Clear",
                              "description": "clear sky",
                              "icon": "01n"},
                 "base": "stations",
                 "main": {"temp": 294.76,
                          "pressure": 1011,
                          "humidity": 78,
                          "temp_min": 290.93,
                          "temp_max": 297.04,
                          "sea_level": 1011,
                          "grnd_level": 1011 },
                 "wind": {"speed": 3.1,
                          "deg": 330},
                 "clouds": {"all": 0},
                 "rain": {"1h": 1,
                          "3h": 2},
                 "snow": {"1h": 1,
                          "3h": 2},
                 "dt": 1575464072,
                 "sys": {"type": 1,
                         "id": 9485,
                         "message ": "test",
                         "country": "AU",
                         "sunrise": 1575398698,
                         "sunset": 1575448256},
                 "timezone": 36000,
                 "id": 0,
                 "visibility": 10000,
                 "name": "Brisbane",
                 "cod": 200}

    columns = flatten(reference)
    os.chdir(dir_path)

    with open(os.path.join(dir_path, "places.yml"), "r") as f:
        places = yaml.load(f)

    # weather_url = 'https://api.openweathermap.org/data/2.5/weather?zip={},{}&APPID={}'
    weather_url = 'https://api.openweathermap.org/data/2.5/weather'
    tbl_name = 'weather'
    run_log = open('download.log', 'a')

    def query_dynamic(zipcode: str, country: str) -> dict:
        r = requests.get(url=weather_url, params={'zip': '{},{}'.format(zipcode, country), 'APPID': apikey})

        weather_data = json.loads(r.text)
        if isinstance(weather_data['weather'], list):
            weather_data['weather'] = weather_data['weather'][0]
        weather_data = flatten(weather_data)
        weather_data['country'] = country
        weather_data['zipcode'] = zipcode
        return weather_data

    def create_connection_and_tables(d):
        output_file = get_output_name(d)
        db = sqlite3.connect(output_file)
        cursor = db.cursor()
        data_fields = list(flatten(reference).keys())
        col_str = '"' + '","'.join(data_fields) + '"'
        wildcards = ','.join(['?'] * len(data_fields))
        cursor.execute("create table  IF NOT EXISTS %s (%s)" % (tbl_name, col_str))
        return output_file, db, cursor, wildcards

    def get_output_name(d):
        d = date.fromtimestamp(d)
        return 'weather-{}-{}-{}.sqlite'.format(d.year, d.month, d.day)

    output_file, db, cur, wildcards = create_connection_and_tables(time())

    while True:
        T = time()
        t = datetime.fromtimestamp(T).strftime("%A, %d. %B %Y %I:%M%p")
        for k, p in places.items():
            zipcode = p['zip']
            country = p['country']
            try:
                weather_data = query_dynamic(zipcode, country)
                weather_data['query_time'] = t
                weather_data['place_name'] = k
                if output_file != get_output_name(T):
                    db.close()
                    output_file, db, cur, wildcards = create_connection_and_tables(time())

                data = [weather_data.get(k) for k in columns]
                col_str = '"' + '","'.join(columns) + '"'
                sql = ''' INSERT INTO {}({}) VALUES({}) '''.format(tbl_name, col_str, wildcards)

                cur.execute(sql, data)

                db.commit()
                run_log.write('Succeed on: {}\n'.format(t))
                print('Success for on {}'.format(t))
            except:
                print('FAILURE for on {}'.format(t))
                run_log.write('Some error at: {}\n'.format(t))

            run_log.flush()
            T = time() - T
            sleep(interval)

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

if __name__ == '__main__':
    args = sys.argv[1:]
    main(*args)
