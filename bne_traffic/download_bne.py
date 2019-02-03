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
import json


def main(apikey, interval):
    interval = float(interval)
    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(dir_path)

    data_url = 'https://www.data.brisbane.qld.gov.au/data/dataset/56e19d91-d571-4b45-bfdf-f0f00aeb2343/resource/651f7be1-c183-48b5-96a8-fe372e91adab/download/traffic-data-at-int.json'
    url_reference = 'https://www.data.brisbane.qld.gov.au/data/dataset/643679f2-2d67-4487-8122-9f632b53cbf0/resource/3cbeb1d6-9bd7-49a4-b4ba-461b045a551f/download/19879.json'
    tbl_name = 'bne_traffic'
    static_tbl_name = 'bne_traffic_static'
    run_log = open('download.log', 'a')
    headers = {'Accept': 'application/json]'}
    cookies = {'auth_tkt': apikey}

    def query_dynamic():
        r = requests.get(url=data_url, cookies=cookies, headers=headers)
        dynamic_data = pd.DataFrame(json.loads(r.text))
        dynamic_data[['ct', 'dbid', 'link_plan', 'ss', 'tsc']] = dynamic_data[
            ['ct', 'dbid', 'link_plan', 'ss', 'tsc']].astype(str)
        return dynamic_data

    def query_static():
        r_ref = requests.get(url=url_reference, cookies=cookies, headers=headers)
        response_ref = json.loads(r_ref.text)

        static_data = []
        for intrs in response_ref:
            try:
                stat_dt = pd.DataFrame(intrs['arms'])
                stat_dt = stat_dt.assign(areaNum=str(intrs['areaNum']))
                stat_dt = stat_dt.assign(region=str(intrs['region']))
                stat_dt = stat_dt.assign(suburb=str(intrs['suburb']))
                stat_dt = stat_dt.assign(tsc=int(intrs['tsc']))
                stat_dt = stat_dt.assign(latitude=float(intrs['coordinates']['latLng']['latitude']))
                stat_dt = stat_dt.assign(longitude=float(intrs['coordinates']['latLng']['longitude']))
                static_data.append(stat_dt)
            except:
                pass
        static_data = pd.concat(static_data, sort=True)
        static_data.drop(['detectors'], axis=1, inplace=True)
        static_data[['arm', 'azimuth', 'leftLaneCount', 'rightLaneCount', 'slipLaneCount', 'tsc']] = static_data[
            ['arm', 'azimuth', 'leftLaneCount', 'rightLaneCount', 'slipLaneCount', 'tsc']].astype(str)
        return static_data

    def create_connection_and_tables(d, dynamic_data, static_data):
        output_file = get_output_name(d)
        db = sqlite3.connect(output_file)
        cur = db.cursor()

        data_fields = list(static_data.columns)
        col_str = '"' + '","'.join(data_fields) + '"'
        wildcards = ','.join(['?'] * len(data_fields))
        cur.execute("create table  IF NOT EXISTS %s (%s)" % (static_tbl_name, col_str))
        cur.executemany("insert into %s values(%s)" % (static_tbl_name, wildcards), static_data.to_records(index=False))

        data_fields = list(dynamic_data.columns)
        col_str = '"' + '","'.join(data_fields) + '"'
        wildcards = ','.join(['?'] * len(data_fields))
        cur.execute("create table  IF NOT EXISTS %s (%s)" % (tbl_name, col_str))

        return output_file, db, cur, wildcards

    def get_output_name(d):
        d = date.fromtimestamp(d)
        return 'bne_traffic-{}-{}-{}.sqlite'.format(d.year, d.month, d.day)

    static_data = query_static()
    dynamic_data = query_dynamic()
    output_file, db, cur, wildcards = create_connection_and_tables(time(), dynamic_data, static_data)
    while True:
        T = time()
        t = datetime.fromtimestamp(T).strftime("%A, %d. %B %Y %I:%M%p")

        try:
            dynamic_data = query_dynamic()
            if output_file != get_output_name(T):
                db.close()
                static_data = query_static()
                output_file, db, cur, wildcards = create_connection_and_tables(time(), dynamic_data, static_data)

            cur.executemany("insert into %s values(%s)" % (tbl_name, wildcards), dynamic_data.to_records(index=False))
            db.commit()
            run_log.write('Succeed on: {}\n'.format(t))
            print('Success for on {}'.format(t))
        except:
            print('FAILURE for on {}'.format(t))
            run_log.write('Some error at: {}\n'.format(t))

        run_log.flush()
        T = time() - T
        print(T)
        sleep(interval * 60 - T)


if __name__ == '__main__':
    args = sys.argv[1:]
    main(*args)
