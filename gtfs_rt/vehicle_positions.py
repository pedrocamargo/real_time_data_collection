import pandas as pd

import requests
from google.transit import gtfs_realtime_pb2 as gtfs_rt


def query_veh_position(veh_url) -> pd.DataFrame:
    feed = gtfs_rt.FeedMessage()
    response = requests.get(veh_url)
    feed.ParseFromString(response.content)

    all_data = []
    for e in feed.entity:  # type:
        if not e.HasField('vehicle'):
            continue

        dt = [e.id, e.vehicle.trip.trip_id, e.vehicle.trip.route_id, e.vehicle.vehicle.label, e.vehicle.current_status,
              e.vehicle.stop_id, e.vehicle.timestamp, e.vehicle.position.latitude, e.vehicle.position.longitude]
        all_data.append(dt)

    cols = ["vehicle_id", "trip_id", "route_id", "label", "status", "stop_id", "timestamp", "latitude", "longitude"]
    return pd.DataFrame(all_data, columns=cols)
