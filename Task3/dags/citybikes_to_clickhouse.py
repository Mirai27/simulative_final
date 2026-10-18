from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, task


with DAG(
    dag_id="anton_mishin_citybikes_to_clickhouse",
    description="Загрузка готовой витрины из HDFS в ClickHouse",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "anton_mishin", "retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["citybikes", "task3"],
) as dag:

    @task
    def load_mart():
        from Task3.load_mart import main

        main()

    load_mart()
