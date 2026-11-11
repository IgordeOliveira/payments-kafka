#!/bin/sh
# Creates the service databases on the first start of an empty data volume.
# Executed by the official postgres image entrypoint (docker-entrypoint-initdb.d).
# Tables are owned by each service's own migrations, not by this script.
set -eu

for db in payments reconciliation notification; do
  echo "Creating database ${db}"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    -c "CREATE DATABASE \"${db}\" OWNER \"${POSTGRES_USER}\";"
done
