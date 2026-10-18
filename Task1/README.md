# Задание 1

DAG anton_mishin_citybikes_to_postgres загружает
сохранённые ответы CityBikes API в PostgreSQL. В приложенной выгрузке от 10.10.2026
содержатся 815 сетей и 4 153 станции из пяти выбранных сетей.

Команды выполнять из корня проекта при запущенном Docker:

```powershell
docker compose up -d --build
```

После готовности сервисов открыть http://localhost:8080, логин и пароль airflow.
Включить DAG anton_mishin_citybikes_to_postgres и нажать Trigger.
Для запуска через командную строку:

```powershell
docker compose exec airflow-scheduler airflow dags unpause anton_mishin_citybikes_to_postgres
docker compose exec airflow-scheduler airflow dags trigger anton_mishin_citybikes_to_postgres
```

Дождаться статуса success. Логи доступны у задачи load_postgres в Airflow.
В базе citybikes появятся таблицы anton_mishin_networks и anton_mishin_stations.
Количество строк можно проверить запросом:

```powershell
docker compose exec postgres psql -U airflow -d citybikes -c "SELECT (SELECT count(*) FROM anton_mishin_networks) AS networks, (SELECT count(*) FROM anton_mishin_stations) AS stations;"
```

Для подключения с компьютера: localhost:5433, база citybikes,
логин и пароль airflow.
После успешного завершения можно запускать задание 2.

Ключ сети - id. Для станции выбран составной ключ (network_id, id),
чтобы одинаковые ID станций в разных сетях не конфликтовали.
network_id берётся из родительского объекта network и связывает таблицы.

Вложенные поля сохраняются в JSONB, пропуски в необязательных полях - в SQL NULL.
timestamp остаётся строкой; даты преобразуются во втором задании.
Таблицы описаны в create_tables.sql, вставка и обновление -
в upsert_networks.sql и upsert_stations.sql.

load_data.py проверяет состав файлов по manifest.json,
затем загружает сети и станции одной транзакцией. При ошибке изменения обеих
таблиц откатываются. Повторная загрузка добавляет новые строки и обновляет
существующие по ключу, без дублей. Записи, отсутствующие в выгрузке, не удаляются.

Ответы API находятся в api_snapshot/. Для обычного запуска скачивать их заново
не нужно. У части сетей старые метки времени: дата скачивания не означает,
что сведения о станциях актуальны на этот день.

При необходимости новую выгрузку можно получить через download_data.py:

```powershell
python Task1/download_data.py --networks divvy velib
```

После успешного скачивания скрипт создаёт manifest.json. Чтобы выбрать эту выгрузку,
указать CITYBIKES_SNAPSHOT=имя_папки в корневом .env и повторить запуск окружения.
Для загрузки нужны список сетей, файлы станций выбранных сетей и manifest.json.

Общий запуск проекта и версии компонентов описаны в [главном README](../README.md).
