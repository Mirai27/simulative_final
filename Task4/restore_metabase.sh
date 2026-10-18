#!/bin/sh
set -eu

database_exists=$(psql -X -At -v ON_ERROR_STOP=1 -c "SELECT 1 FROM pg_database WHERE datname = 'metabase'")
if [ "$database_exists" = "1" ]; then
    echo "Metabase database already exists; keeping saved settings."
    exit 0
fi

# Публикуем базу только после успешного восстановления.
dropdb --if-exists metabase_restore
createdb --owner="$PGUSER" metabase_restore
pg_restore --dbname=metabase_restore --no-owner --no-acl --single-transaction /backup/metabase.dump
psql -X -v ON_ERROR_STOP=1 -c "ALTER DATABASE metabase_restore RENAME TO metabase"
echo "Metabase database restored. Dashboard: http://localhost:3000/dashboard/2"
