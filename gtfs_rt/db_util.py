import sqlite3
from datetime import date


def get_output_name(d):
    d = date.fromtimestamp(d)
    return '-{}-{}-{}.sqlite'.format(d.year, d.month, d.day)


def create_connection_and_table(d, dir_path, file_prefix):
    output_file_stub = get_output_name(d)
    output_file = dir_path / f"{file_prefix}{output_file_stub}"
    db = sqlite3.connect(output_file)

    sql1 = """CREATE TABLE IF NOT EXISTS trip_update(
                                                     place_name TEXT,
                                                     trip_id, TEXT,
                                                     route_id TEXT,
                                                     vehicle_id TEXT,
                                                     stop_id TEXT,
                                                     delay NUMERIC,
                                                     timestamp NUMERIC,
                                                     uncertainty NUMERIC,
                                                     schedule_relationship NUMERIC,
                                                     query_time  NUMERIC);"""

    sql2 = """CREATE TABLE IF NOT EXISTS vehicle_position(
                                                          place_name TEXT,
                                                          vehicle_id TEXT,
                                                          trip_id, TEXT,
                                                          route_id TEXT,
                                                          label TEXT,
                                                          status INTEGER,
                                                          stop_id TEXT,
                                                          timestamp NUMERIC,
                                                          latitude NUMERIC,
                                                          longitude NUMERIC,
                                                          query_time  NUMERIC);"""

    for sql in [sql1, sql2]:
        db.execute(sql)
    db.commit()

    return output_file_stub, db
