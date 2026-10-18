# Задание 3

DAG anton_mishin_citybikes_to_clickhouse переносит готовую витрину из HDFS
в ClickHouse. Загрузка находится в load_mart.py,
файл DAG - dags/citybikes_to_clickhouse.py.

Перед запуском выполнить задание 2 и дождаться статуса success.
Предыдущие этапы здесь не запускаются.

Запуск из корня проекта:

```powershell
docker compose exec airflow-scheduler airflow dags unpause anton_mishin_citybikes_to_clickhouse
docker compose exec airflow-scheduler airflow dags trigger anton_mishin_citybikes_to_clickhouse
```

Также DAG можно включить и запустить через http://localhost:8080,
логин и пароль airflow. Дождаться статуса success.
Количество загруженных строк доступно в логе задачи load_mart.

Результат - таблица citybikes.anton_mishin_dm: 4 153 строки и 26 столбцов
для приложенной выгрузки. Схема и типы заданы в create_table.sql.
Проверить количество строк:

```powershell
docker compose exec clickhouse clickhouse-client --user anton_mishin --password citybikes --database citybikes --query "SELECT count() FROM anton_mishin_dm"
```

Таблица anton_mishin_dm_hdfs читает part-*.json из /anton_mishin/dm
через HDFS Engine в формате JSONEachRow. INSERT SELECT копирует данные
в MergeTree; схема создаётся через clickhouse_connect.
Spark и JDBC-драйвер ClickHouse для этого этапа не нужны.
Сортировка по сети и названию станции не ограничивает уникальность строк.

NULL в network_company и station_payment заменяется на []:
ClickHouse не поддерживает Nullable для массива целиком.
Пустой список не доказывает отсутствие компаний или способов оплаты.
Остальные пропуски сохраняются, а строки без network_id отклоняются.
companies_count переносится без пересчёта.

Обе даты имеют тип DateTime('UTC') с точностью до секунды.
Для station_last_updated UTC принят условно: исходные часы сохраняются,
часовой пояс источника не подтверждён.

Повторный запуск заменяет витрину целиком. Сначала загружается таблица
anton_mishin_dm_loading. После проверки она меняется местами с основной
через EXCHANGE TABLES, затем прежняя копия удаляется.
Пустой источник или ошибка загрузки не заменяет основную витрину.
Промежуточная таблица удаляется и при ошибке; после аварийного прерывания
её уберёт следующий запуск. Этапы проекта запускаются по очереди.

После success можно переходить к заданию 4. Окружение и доступ к сервисам
описаны в [главном README](../README.md).
