"""Загрузить готовую витрину из HDFS в ClickHouse."""

import logging
import os
from contextlib import closing
from pathlib import Path

import clickhouse_connect


logger = logging.getLogger(__name__)


def connect():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8124")),
        database=os.getenv("CLICKHOUSE_DATABASE", "citybikes"),
        username=os.getenv("CLICKHOUSE_USER", "anton_mishin"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "citybikes"),
    )


def load_mart(client, path):
    sql = Path(__file__).with_name("create_table.sql").read_text(encoding="utf-8")
    client.command(sql)
    client.command("DROP TABLE IF EXISTS anton_mishin_dm_hdfs")
    client.command("""
        CREATE TABLE anton_mishin_dm_hdfs AS anton_mishin_dm
        ENGINE = HDFS({path:String}, 'JSONEachRow')
    """, parameters={"path": path.rstrip("/") + "/part-*.json"})
    logger.info("Источник HDFS: %s", path)

    # Сначала загружаем новую копию, чтобы при ошибке сохранить старую витрину.
    client.command("DROP TABLE IF EXISTS anton_mishin_dm_loading")
    client.command("CREATE TABLE anton_mishin_dm_loading AS anton_mishin_dm")
    try:
        client.command("""
            INSERT INTO anton_mishin_dm_loading
            SELECT * FROM anton_mishin_dm_hdfs
            SETTINGS input_format_null_as_default = 1
        """)
        count, missing_ids = client.query("""
            SELECT count(), countIf(network_id = '')
            FROM anton_mishin_dm_loading
        """).first_row
        if count == 0:
            raise ValueError("Витрина HDFS пуста. Сначала выполните DAG задания 2: anton_mishin_citybikes_dm.")
        if missing_ids:
            raise ValueError("В витрине HDFS есть строки без network_id.")

        client.command("EXCHANGE TABLES anton_mishin_dm AND anton_mishin_dm_loading")
    finally:
        client.command("DROP TABLE IF EXISTS anton_mishin_dm_loading")
    logger.info("Витрина загружена в ClickHouse: anton_mishin_dm, строк: %d", count)
    return count


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    with closing(connect()) as client:
        path = os.getenv("CITYBIKES_DM_PATH", "hdfs://namenode:9000/anton_mishin/dm")
        load_mart(client, path)


if __name__ == "__main__":
    main()
