INSERT INTO anton_mishin_stations
    (network_id, id, name, latitude, longitude, timestamp, free_bikes, empty_slots, extra)
VALUES %s
ON CONFLICT (network_id, id) DO UPDATE SET
    name = EXCLUDED.name, latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude, timestamp = EXCLUDED.timestamp,
    free_bikes = EXCLUDED.free_bikes, empty_slots = EXCLUDED.empty_slots,
    extra = EXCLUDED.extra;
