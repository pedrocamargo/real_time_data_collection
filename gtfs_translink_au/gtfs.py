import sys

# We try to install requests and gtfs-rt bindings because the default Python 3.6 docker images don't have these packages
# If you deploy your own images, then you don't need these, but if you don't, you definitely do. No harm in keeping it
try:
    import pip

    for p in ['gtfs-realtime-bindings', 'requests']:
        pip.main(['install', p])
except:
    print("pip not available")

from google.transit import gtfs_realtime_pb2 as gtfs_rt
import requests
import sqlite3
from datetime import date, datetime
from time import sleep, time
import os


def main(interval):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(dir_path)
    gtfs_rt_url = 'https://gtfsrt.api.translink.com.au/Feed/SEQ'
    run_log = open('bne_gtfs.log', 'w')

    tbl_name = 'brisbane'

    def create_connection_and_table(d):
        output_file = get_output_name(d)
        db = sqlite3.connect(output_file)
        cur = db.cursor()

        col_str = '"vehicle_id","trip_id","route_id","timestamp","delay", "latitude","longitude", "query_time"'
        wildcards = ','.join(['?'] * 8)
        cur.execute("create table  IF NOT EXISTS %s (%s)" % (tbl_name, col_str))
        return output_file, db, cur, wildcards

    def get_output_name(d):
        d = date.fromtimestamp(d)
        return 'gtfs_bne-{}-{}-{}.sqlite'.format(d.year, d.month, d.day)

    output_file, db, cur, wildcards = create_connection_and_table(time())
    while True:
        T = time()
        t = time()
        if output_file != get_output_name(t):
            db.close()
            output_file, db, cur, wildcards = create_connection_and_table(t)
        tm = datetime.fromtimestamp(t).strftime("%A, %d. %B %Y %I:%M%p")
        try:
            feed = gtfs_rt.FeedMessage()
            response = requests.get(gtfs_rt_url)
            feed.ParseFromString(response.content)

            vehicles = {}
            trips = {}
            for entity in feed.entity:  # type:
                if entity.HasField('trip_update'):
                    trips[entity.trip_update.vehicle.id] = entity.trip_update
                v = entity.vehicle
                if not v.trip.route_id:
                    continue
                vehicles[v.vehicle.id] = v
            data = []
            for veh_id in list(vehicles.keys()):
                my_vehicle = vehicles[veh_id]
                delay = 0

                if veh_id in trips:
                    my_trip = trips[veh_id]
                    veh_stop = my_vehicle.stop_id
                    for stop in my_trip.stop_time_update:
                        if stop.stop_id == veh_stop:
                            delay = 0.5 * (stop.arrival.delay + stop.departure.delay)

                timestamp = datetime.fromtimestamp(my_vehicle.timestamp).strftime('%Y-%m-%d %H:%M:%S')
                trip_id = my_vehicle.trip.trip_id
                route_id = my_vehicle.trip.route_id
                latitude = my_vehicle.position.latitude
                longitude = my_vehicle.position.longitude
                data.append((veh_id, trip_id, route_id, timestamp, delay, latitude, longitude, t))

            cur.executemany("insert into %s values(%s)" % (tbl_name, wildcards), data)
            db.commit()
            run_log.write('\nSucceed on: {}'.format(tm))
            print('Success on {}'.format(tm))
        except:
            print('\nFAILURE on {}'.format(tm))
            run_log.write('\nSome error at: {}'.format(tm))
        run_log.flush()
        T = time() - T
        sleep(max(interval * 60 - T, 1))


if __name__ == '__main__':
    interval = sys.argv[1]
    main(interval)
