"""Построить витрину CityBikes из PostgreSQL и сохранить её в HDFS."""

import logging
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType, StructField, StructType,
)


logger = logging.getLogger(__name__)

LOCATION_SCHEMA = StructType([
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
])

EXTRA_SCHEMA = StructType([
    # В источнике renting/returning встречаются как 0/1 и как true/false.
    StructField("renting", StringType()),
    StructField("returning", StringType()),
    StructField("last_updated", StringType()),
    StructField("slots", IntegerType()),
    StructField("ebikes", IntegerType()),
    StructField("has_ebikes", StringType()),
    StructField("payment", ArrayType(StringType())),
    StructField("payment-terminal", StringType()),
    StructField("address", StringType()),
    StructField("post_code", StringType()),
])

JSON_OPTIONS = {
    "timestampFormat": "yyyy-MM-dd HH:mm:ss",
    "timestampNTZFormat": "yyyy-MM-dd HH:mm:ss",
    "timeZone": "UTC",
}


def read_table(spark, table):
    host = os.getenv("PGHOST", "postgres")
    port = os.getenv("PGPORT", "5432")
    database = os.getenv("PGDATABASE", "citybikes")
    return spark.read.jdbc(
        url=f"jdbc:postgresql://{host}:{port}/{database}",
        table=table,
        properties={
            "user": os.getenv("PGUSER", "airflow"),
            "password": os.getenv("PGPASSWORD", "airflow"),
            "driver": "org.postgresql.Driver",
        },
    )


def boolean_value(value):
    value = F.lower(F.trim(value))
    return F.when(value.isin("true", "1"), F.lit(True)).when(value.isin("false", "0"), F.lit(False))


def datetime_value(value, without_timezone=False):
    value = F.trim(value.cast("string"))
    # API иногда добавляет Z после уже указанного смещения +00:00.
    value = F.regexp_replace(value, r"([+-]\d{2}:\d{2})Z$", "$1")
    unix_time = F.when(value.rlike(r"^\d{10}$"), F.timestamp_seconds(value.cast("long")))
    unix_time = unix_time.when(value.rlike(r"^\d{13}$"), F.timestamp_millis(value.cast("long")))
    parsed = F.try_to_timestamp(value)
    if without_timezone:
        # Для дат без пояса сохраняем исходное время. Известное смещение приводим к UTC.
        parsed = F.when(
            value.rlike(r"(Z|[+-]\d{2}:?\d{2})$"), parsed.cast("timestamp_ntz")
        ).otherwise(F.to_timestamp_ntz(value))
        parsed = F.coalesce(unix_time.cast("timestamp_ntz"), parsed)
        return F.date_trunc("second", parsed).cast("timestamp_ntz")
    return F.date_trunc("second", F.coalesce(unix_time, parsed))


def field(path, transform=None):
    """Взять поле, при необходимости обработать и дать имя для витрины."""
    source, *parts = path.split(".")
    prefix = {"n": "network", "s": "station"}[source]
    parts = [part for part in parts if part not in ("location", "extra")]
    name = "_".join([prefix, *parts]).replace("-", "_")
    value = F.col(path)
    if transform is not None:
        value = transform(value)
    return value.alias(name)


def build_mart(networks, stations):
    networks = networks.withColumn("location", F.from_json("location", LOCATION_SCHEMA, {"mode": "FAILFAST"}))
    networks = networks.withColumn("company", F.from_json("company", ArrayType(StringType()), {"mode": "FAILFAST"}))
    stations = stations.withColumn("extra", F.from_json("extra", EXTRA_SCHEMA, {"mode": "FAILFAST"}))
    joined = stations.alias("s").join(networks.alias("n"), F.col("s.network_id") == F.col("n.id"), "inner")

    mart = joined.select(
        field("n.id", F.trim),
        field("n.name", F.trim),
        field("n.location.latitude"),
        field("n.location.longitude"),
        field("n.location.city", F.trim),
        field("n.location.country", F.trim),
        field("n.company", lambda value: F.transform(value, F.trim)),
        field("n.system", F.trim),
        field("s.name", F.trim),
        field("s.latitude", lambda value: value.cast("double")),
        field("s.longitude", lambda value: value.cast("double")),
        field("s.timestamp", datetime_value),
        field("s.free_bikes", lambda value: value.cast("int")),
        field("s.empty_slots", lambda value: value.cast("int")),
        field("s.extra.renting", boolean_value),
        field("s.extra.returning", boolean_value),
        field("s.extra.last_updated", lambda value: datetime_value(value, without_timezone=True)),
        field("s.extra.slots"),
        field("s.extra.ebikes"),
        field("s.extra.has_ebikes", boolean_value),
        field("s.extra.payment", lambda value: F.transform(value, F.trim)),
        field("s.extra.payment-terminal", boolean_value),
        field("s.extra.address", F.trim),
        field("s.extra.post_code", F.trim),
    )
    return mart.withColumn(
        "companies_count", F.when(F.col("network_company").isNotNull(), F.size("network_company"))
    ).withColumn("network_station", F.concat("network_name", F.lit("/"), "station_name"))


def write_mart(mart, path):
    """Записать новую витрину и заменить прежнюю после успешной записи."""
    spark = mart.sparkSession
    target = spark._jvm.org.apache.hadoop.fs.Path(path)
    if not target.isAbsolute() or not target.getName():
        raise ValueError("Для витрины нужен абсолютный путь к каталогу, не корень файловой системы.")
    fs = target.getFileSystem(spark._jsc.hadoopConfiguration())
    staging = target.suffix(".staging")
    previous = target.suffix(".previous")

    # Восстановление после прерванной замены каталога.
    if fs.exists(previous):
        if fs.exists(target):
            if not fs.delete(previous, True):
                raise OSError(f"Не удалось удалить прежнюю копию: {previous}")
        elif not fs.rename(previous, target):
            raise OSError(f"Не удалось восстановить витрину из {previous}")

    mart.write.mode("overwrite").options(**JSON_OPTIONS).option("ignoreNullFields", "false").json(staging.toString())
    had_previous = fs.exists(target)
    if had_previous and not fs.rename(target, previous):
        raise OSError(f"Не удалось сохранить прежнюю витрину: {target}")
    try:
        if not fs.rename(staging, target):
            raise OSError(f"Не удалось заменить витрину: {target}")
    except Exception:
        if had_previous and not fs.exists(target) and not fs.rename(previous, target):
            raise OSError(f"Не удалось восстановить витрину. Прежние данные находятся в {previous}")
        raise
    if had_previous and not fs.delete(previous, True):
        raise OSError(f"Витрина обновлена, но не удалось удалить прежнюю копию: {previous}")


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    spark = (
        SparkSession.builder.appName("citybikes_dm")
        .master("local[2]")
        .config("spark.jars", os.getenv("POSTGRES_JDBC_JAR", "/opt/airflow/jars/postgresql.jar"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.dfs.replication", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        networks = read_table(spark, "anton_mishin_networks")
        stations = read_table(spark, "anton_mishin_stations")
        mart = build_mart(networks, stations).cache()
        count = mart.count()
        if count == 0:
            raise ValueError("Витрина пуста. Сначала загрузите данные DAG задания 1: anton_mishin_citybikes_to_postgres.")
        output = os.getenv("CITYBIKES_DM_PATH", "hdfs://namenode:9000/anton_mishin/dm")
        write_mart(mart, output)
        logger.info("Витрина сохранена: %s, строк: %d, столбцов: %d", output, count, len(mart.columns))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
