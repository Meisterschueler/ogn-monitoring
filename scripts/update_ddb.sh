#!/bin/bash

echo --- Started DDB import ---

echo Downloading DDB csv file
wget http://ddb.glidernet.org/download/?t=1 -O /ressources/ddb.csv

echo Write csv file to database into temporary table
csvsql --db postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} --tables ddb_csvkit --insert --overwrite /ressources/ddb.csv

echo Migrate data from temporary table to final table
read -d '' sql << EOF
    TRUNCATE ddb;
    INSERT INTO ddb (address,address_type,model,model_type,registration,cn,is_notrack,is_noident)
    SELECT
        ('x' || lpad("DEVICE_ID", 8, '0'))::bit(32)::int AS address,
        CASE "#DEVICE_TYPE"
            WHEN 'I' THEN 1
            WHEN 'F' THEN 2
            WHEN 'O' THEN 3
        END AS address_type,
        "AIRCRAFT_MODEL" AS model,
        CAST("AIRCRAFT_TYPE" AS SMALLINT) AS model_type,
        "REGISTRATION" AS registration,
        "CN" AS cn,
        "TRACKED" = 'N' AS is_notrack,
        "IDENTIFIED" = 'N' AS is_noident
    FROM ddb_csvkit AS di;
    REFRESH MATERIALIZED VIEW ddb_joined;
EOF

psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -c "$sql"

echo Refresh materialized view registration_joined
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -c "REFRESH MATERIALIZED VIEW registration_joined;"

echo --- Finished DDB import ---
