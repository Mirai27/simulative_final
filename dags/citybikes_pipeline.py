from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, task


with DAG(
    dag_id="anton_mishin_citybikes_pipeline",
    description="Загрузка данных для дашборда: JSON, PostgreSQL, HDFS и ClickHouse",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "anton_mishin", "retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["citybikes", "pipeline"],
) as dag:

    @task
    def load_postgres():
        from Task1.load_data import load_snapshot

        load_snapshot()

    @task
    def create_mart():
        from Task2.build_mart import main

        main()

    @task
    def load_clickhouse():
        from Task3.load_mart import main

        main()

    load_postgres() >> create_mart() >> load_clickhouse()
