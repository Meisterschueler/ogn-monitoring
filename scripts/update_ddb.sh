#!/bin/bash

CURRENT_PATH="$(dirname $BASH_SOURCE)"

echo Downloading DDB
wget http://ddb.glidernet.org/download/?t=1 -O ddb.csv

echo Migrate import to final table
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f ${CURRENT_PATH}/update_ddb.sql


echo Finished!
while true; do sleep 60; echo "sleeping"; done
