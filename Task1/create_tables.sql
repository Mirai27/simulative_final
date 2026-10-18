CREATE TABLE IF NOT EXISTS anton_mishin_networks (
    id TEXT PRIMARY KEY,
    name TEXT,
    location JSONB,
    href TEXT,
    company JSONB,
    gbfs_href TEXT,
    system TEXT,
    source TEXT,
    ebikes BOOLEAN,
    license JSONB,
    scooters BOOLEAN,
    instances JSONB
);

CREATE TABLE IF NOT EXISTS anton_mishin_stations (
    network_id TEXT REFERENCES anton_mishin_networks(id),
    id TEXT,
    name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timestamp TEXT,
    free_bikes INTEGER,
    empty_slots INTEGER,
    extra JSONB,
    PRIMARY KEY (network_id, id)
);
