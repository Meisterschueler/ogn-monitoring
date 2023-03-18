#!/bin/bash

CURRENT_PATH="$(dirname $BASH_SOURCE)"

echo Downloading DDB
wget http://api.weglide.org/v1/user/device -O weglide.json

echo Converting json to csv
cat weglide.json | jq -r '.[] | {address: .id, registration: .name, cn: .competition_id, model: .aircraft.name, until: .until, pilot: .user.name} | join(",")' > weglide.csv

echo Migrate import to final table
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f ${CURRENT_PATH}/update_weglide.sql

echo Finished!
while true; do sleep 60; echo "sleeping"; done
