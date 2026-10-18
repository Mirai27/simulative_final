CREATE TABLE IF NOT EXISTS anton_mishin_dm
(
    network_id String,
    network_name Nullable(String),
    network_latitude Nullable(Float64),
    network_longitude Nullable(Float64),
    network_city Nullable(String),
    network_country Nullable(String),
    network_company Array(String),
    network_system Nullable(String),
    station_name Nullable(String),
    station_latitude Nullable(Float64),
    station_longitude Nullable(Float64),
    station_timestamp Nullable(DateTime('UTC')),
    station_free_bikes Nullable(Int32),
    station_empty_slots Nullable(Int32),
    station_renting Nullable(Bool),
    station_returning Nullable(Bool),
    station_last_updated Nullable(DateTime('UTC')),
    station_slots Nullable(Int32),
    station_ebikes Nullable(Int32),
    station_has_ebikes Nullable(Bool),
    station_payment Array(String),
    station_payment_terminal Nullable(Bool),
    station_address Nullable(String),
    station_post_code Nullable(String),
    companies_count Nullable(Int32),
    network_station Nullable(String)
)
ENGINE = MergeTree
ORDER BY (network_id, ifNull(station_name, ''));
