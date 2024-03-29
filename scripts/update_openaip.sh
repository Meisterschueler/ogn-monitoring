#!/bin/bash

echo --- Started OpenAIP import ---

echo Download OpenAIP airport cup files
./get_openaip.py

echo Merge all cup files to one csv file
awk 'FNR == 1 && NR != 1 {next} /^,*$/ {next} !seen[$0]++ ' /ressources/*.cup > /ressources/openaip.csv

echo Write csv to database
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f /scripts/update_openaip.sql

echo --- Finished OpenAIP import ---
