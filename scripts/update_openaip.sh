#!/bin/bash

echo Download OpenAIP airport files
./get_openaip.py

echo Merge all .cup files to one .csv file
awk 'FNR == 1 && NR != 1 {next} /^,*$/ {next} !seen[$0]++ ' *.cup > openaip.csv

echo Write csv to database
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f update_openaip.sql

echo Finished!
while true; do sleep 60; echo "sleeping"; done
