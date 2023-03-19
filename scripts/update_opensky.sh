#!/bin/bash

CURRENT_PATH="$(dirname $BASH_SOURCE)"

echo Downloading OpenSky
#wget http://opensky-network.org/datasets/metadata/aircraftDatabase.csv -O opensky_aircraft_database.csv

echo Write csv to database
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f ${CURRENT_PATH}/update_opensky.sql

echo Finished OpenSky import