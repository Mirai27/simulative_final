"""Загрузить сохранённые ответы CityBikes API в PostgreSQL."""

import argparse
import json
import logging
import os
from contextlib import closing
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values


DEFAULT_DATA_DIR = Path(__file__).resolve().parent.parent / "api_snapshot" / "2026-10-10_14-10-19_826933_UTC"
logger = logging.getLogger(__name__)


def data_directory():
    return Path(os.getenv("CITYBIKES_DATA_DIR", str(DEFAULT_DATA_DIR)))


def json_value(value):
    # None передаём напрямую, чтобы получить SQL NULL, а не JSON null.
    return Json(value) if value is not None else None


def read_sql(filename):
    return Path(__file__).with_name(filename).read_text(encoding="utf-8")


def read_snapshot(directory):
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError(f"В {directory} нет manifest.json. Используйте завершённую выгрузку.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("status", "complete") != "complete":
        raise ValueError("Выгрузка не завершена.")
    selected = manifest.get("selected_network_ids")
    if not isinstance(selected, list) or not selected:
        raise ValueError("В выгрузке не выбраны сети со станциями.")
    paths = sorted(directory.glob("network_*.json"))
    if {path.name for path in paths} != {f"network_{network_id}.json" for network_id in selected}:
        raise ValueError("Файлы станций не соответствуют списку сетей в manifest.json.")

    networks = json.loads((directory / "networks.json").read_text(encoding="utf-8"))["networks"]
    if not isinstance(networks, list) or any(not isinstance(n, dict) for n in networks):
        raise ValueError("В networks.json ожидается массив объектов networks.")
    stations = []
    for path in paths:
        network = json.loads(path.read_text(encoding="utf-8"))["network"]
        if not isinstance(network, dict) or path.name != f"network_{network.get('id')}.json":
            raise ValueError(f"ID сети не соответствует файлу {path.name}.")
        if not isinstance(network["stations"], list) or any(not isinstance(s, dict) for s in network["stations"]):
            raise ValueError(f"В {path.name} ожидается массив объектов stations.")
        stations.extend({**station, "network_id": network["id"]} for station in network["stations"])
    return networks, stations


def load_snapshot(directory=None):
    networks, stations = read_snapshot(Path(directory) if directory is not None else data_directory())
    network_rows = [
        (
            network["id"], network.get("name"), json_value(network.get("location")),
            network.get("href"), json_value(network.get("company")), network.get("gbfs_href"),
            network.get("system"), network.get("source"), network.get("ebikes"),
            json_value(network.get("license")), network.get("scooters"), json_value(network.get("instances")),
        )
        for network in networks
    ]
    station_rows = [
        (
            station["network_id"], station["id"], station.get("name"),
            station.get("latitude"), station.get("longitude"), station.get("timestamp"),
            station.get("free_bikes"), station.get("empty_slots"), json_value(station.get("extra")),
        )
        for station in stations
    ]
    with closing(psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"), port=os.getenv("PGPORT", "5433"),
        dbname=os.getenv("PGDATABASE", "citybikes"), user=os.getenv("PGUSER", "airflow"),
        password=os.getenv("PGPASSWORD", "airflow"), connect_timeout=10,
    )) as connection:
        with connection, connection.cursor() as cursor:
            cursor.execute(read_sql("create_tables.sql"))
            execute_values(cursor, read_sql("upsert_networks.sql"), network_rows, page_size=500)
            execute_values(cursor, read_sql("upsert_stations.sql"), station_rows, page_size=500)
    logger.info("Загружено сетей: %d, станций: %d", len(network_rows), len(station_rows))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=data_directory())
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        load_snapshot(args.data_dir)
    except (OSError, ValueError, KeyError, psycopg2.Error):
        logger.exception("Загрузка не завершена")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
