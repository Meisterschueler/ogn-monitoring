#!/bin/bash

echo Downloading OpenSky
wget http://opensky-network.org/datasets/metadata/aircraft-database-complete-2023-03.csv -O /ressources/opensky_aircraft_database.csv

echo Write csv to database
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f /scripts/update_opensky.sql

echo Finished OpenSky import
