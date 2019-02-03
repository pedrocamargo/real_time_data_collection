import sys

# We try to install requests and pandas because the default Python 3.6 docker images do not have these packages
# If you deploy your own images, then you don't need these, but if you don;t, you definitely do. No harm in keeping it
try:
    import pip

    for p in ['requests', 'pandas']:
        pip.main(['install', p])
except:
    print("pip not available")

import requests
from time import sleep, time
from datetime import datetime, date
import sqlite3
import pandas as pd
import os
import csv


def main(cycle_api, interval, city=None):
    interval = float(interval)
    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(dir_path)
    cities_list = 'cities.csv'
    cycle_url = 'https://api.jcdecaux.com/vls/v1/stations'
    tbl_name = 'jcdecaux'
    run_log = open('download.log', 'a')

    with open(cities_list, newline='') as csvfile:
        c = csv.reader(csvfile, delimiter=' ', quotechar='|')
        cities = [x for x in c]

    # download only the city/cities you want
    if city is None:
        print('downloading for all cities')
    else:
        print("Downloading only for {}".format(city))
        if isinstance(city, str):
            cities = [city]
        else:
            cities = city
    contract_name = cities[0]

    def do_request(contract_name):
        return requests.get(url=cycle_url, params={'contract': contract_name, 'apiKey': cycle_api})

    def builds_dataframe(req):
        t = time()
        q = req.json()
        for i in q:
            i['query_time'] = t
            i['latitude'] = i['position']['lat']
            i['longitude'] = i['position']['lng']
            i.pop('position')
        return pd.DataFrame(q)

    def create_connection_and_table(d):
        output_file = get_output_name(d)
        db = sqlite3.connect(output_file)
        cur = db.cursor()
        bike_stations = builds_dataframe(do_request(contract_name))

        col_str = '"' + '","'.join(bike_stations.columns) + '"'
        wildcards = ','.join(['?'] * len(bike_stations.columns))
        cur.execute("create table  IF NOT EXISTS %s (%s)" % (tbl_name, col_str))
        return output_file, db, cur, wildcards

    def get_output_name(d):
        d = date.fromtimestamp(d)
        return 'jcdecaux-{}-{}-{}.sqlite'.format(d.year, d.month, d.day)

    output_file, db, cur, wildcards = create_connection_and_table(time())
    while True:
        for contract_name in cities:
            t = time()
            if output_file != get_output_name(t):
                db.close()
                output_file, db, cur, wildcards = create_connection_and_table(t)
            t = datetime.fromtimestamp(t).strftime("%A, %d. %B %Y %I:%M%p")
            try:
                sleep(interval * 60 / len(cities))
                r = do_request(contract_name)
                bike_stations = builds_dataframe(r)
                data = [tuple(x) for x in bike_stations.values]

                cur.executemany("insert into %s values(%s)" % (tbl_name, wildcards), data)
                db.commit()
                run_log.write('Succeed on: {}\n'.format(t))
                print('Success for {} on {}'.format(contract_name, t))
            except:
                print('FAILURE for {} on {}'.format(contract_name, t))
                run_log.write('Some error at: {}\n'.format(t))

        run_log.flush()


if __name__ == '__main__':
    args = sys.argv[1:]
    main(*args)
