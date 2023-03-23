#!/bin/bash

echo Downloading WeGlide
wget http://api.weglide.org/v1/user/device -O /ressources/weglide.json

echo Converting json to csv
cat /ressources/weglide.json | jq -r '.[] | {address: .id, registration: .name, cn: .competition_id, model: .aircraft.name, until: .until, pilot: .user.name} | join(",")' > /ressources/weglide.csv

echo Write import to database
csvsql --db postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} --tables weglide_csvkit --no-header-row --insert --overwrite /ressources/weglide.csv

echo Migrate data from import table to final table
read -d '' sql << EOF
    TRUNCATE weglide;
    INSERT INTO weglide (address,registration,cn,model,until,pilot)
    SELECT
        ('x' || lpad(w.a, 8, '0'))::bit(32)::int AS address,
        w.b AS registration,
        w.c AS cn,
        w.d AS model,
        w.e::TIMESTAMPTZ AS until,
        w.f AS pilot
FROM weglide_csvkit AS w;
EOF
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -c "$sql"

echo Finished WeGlide import
