import pandas as pd

import requests
from google.transit import gtfs_realtime_pb2 as gtfs_rt


def query_trip_updates(trips_url) -> pd.DataFrame:
    feed = gtfs_rt.FeedMessage()
    response = requests.get(trips_url)
    feed.ParseFromString(response.content)

    all_data = []
    for entity in feed.entity:  # type:
        if not entity.HasField('trip_update'):
            continue
        e = entity.trip_update

        for stu in [x for x in e.stop_time_update]:
            dt = [e.trip.trip_id, e.trip.route_id, e.vehicle.id, stu.stop_id, stu.departure.delay, stu.departure.time,
                  stu.departure.uncertainty, stu.schedule_relationship]
            all_data.append(dt)

    cols = ["trip_id", "route_id", "vehicle_id", "stop_id", "delay", "timestamp", "uncertainty",
            "schedule_relationship"]
    return pd.DataFrame(all_data, columns=cols)
