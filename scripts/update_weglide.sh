#!/bin/bash

echo Downloading WeGlide
wget http://api.weglide.org/v1/user/device -O /ressources/weglide.json

echo Converting json to csv
cat weglide.json | jq -r '.[] | {address: .id, registration: .name, cn: .competition_id, model: .aircraft.name, until: .until, pilot: .user.name} | join(",")' > /ressources/weglide.csv

echo Write import to database
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f /scripts/update_weglide.sql

echo Finished WeGlide import