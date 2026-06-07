#!/bin/bash
# create_multiple_databases.sh
# Runs at PostgreSQL first-start to create airflow_db and superset_db
# alongside the primary customer360_warehouse database.
# Place in /docker-entrypoint-initdb.d/ — runs AFTER the primary DB is created.

set -e

function create_db() {
    local database=$1
    echo "Creating database: $database"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        CREATE DATABASE $database OWNER $POSTGRES_USER;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $POSTGRES_USER;
EOSQL
}

# Create additional databases if they don't exist
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Multiple databases requested: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo "$POSTGRES_MULTIPLE_DATABASES" | tr ',' ' '); do
        # Skip the primary database which already exists
        if [ "$db" != "$POSTGRES_DB" ]; then
            create_db "$db"
        fi
    done
    echo "All databases created successfully."
fi
