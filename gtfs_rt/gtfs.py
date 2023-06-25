import json
import traceback
from datetime import datetime
from os.path import dirname, realpath
from pathlib import Path
from time import sleep, time

from gtfs_rt.db_util import create_connection_and_table, get_output_name
from gtfs_rt.setup_environment import all_done
from gtfs_rt.trip_updates import query_trip_updates
from gtfs_rt.vehicle_positions import query_veh_position


def main(config_path):
    with open(config_path, "r") as f:
        config = json.loads(f.read())

    tbl_name = config["dbname"]
    trips_url = config.get("trip_update_url")
    veh_url = config.get("veh_position_url")
    interval = config.get("time_interval", 1)

    dir_path = Path(config.get("output_folder", dirname(realpath(__file__))))
    if not trips_url and not veh_url:
        return

    run_log = open(dir_path / f'{tbl_name}.log', 'w')

    output_file_stub, res_conn = create_connection_and_table(time(), dir_path, tbl_name)
    while True:
        t = time()

        if output_file_stub != get_output_name(t):
            res_conn.close()
            output_file_stub, res_conn = create_connection_and_table(t, dir_path, tbl_name)

        tm = datetime.fromtimestamp(t).strftime("%A, %d. %B %Y %I:%M%p")
        try:
            if veh_url:
                vehicle_positions = query_veh_position(veh_url).assign(place_name=tbl_name, query_time=t)
                vehicle_positions.to_sql("vehicle_position", res_conn, if_exists="append", index=False)
            if trips_url:
                trip_updates = query_trip_updates(trips_url).assign(place_name=tbl_name, query_time=t)
                trip_updates.to_sql("trip_update", res_conn, if_exists="append", index=False)
            print('Success on {}'.format(tm))
        except:
            print('\nFAILURE on {}'.format(tm))
            run_log.write('\nSome error at: {}'.format(tm))
            run_log.write(traceback.format_exc())
        run_log.flush()
        sleep(interval * 60)


if __name__ == '__main__':
    if all_done:
        main(r"D:\src\real_time_data_collection\gtfs_rt\config.json")
