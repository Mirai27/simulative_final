from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, task


with DAG(
    dag_id="anton_mishin_citybikes_dm",
    description="Построение витрины из PostgreSQL и сохранение в HDFS",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "anton_mishin", "retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["citybikes", "task2"],
) as dag:

    @task
    def create_mart():
        from Task2.build_mart import main

        main()

    create_mart()
