#!/bin/bash

echo --- Started timezones import ---

echo Download countries zip file
wget https://github.com/evansiroky/timezone-boundary-builder/releases/download/2024a/timezones-with-oceans-now.shapefile.zip -O /ressources/timezones-with-oceans-now.shapefile.zip

echo Unzipping archive
unzip /ressources/timezones-with-oceans-now.shapefile.zip -o -d /ressources

echo Converting shp to sql
shp2pgsql -d -s 4326 -I -D /ressources/combined-shapefile-with-oceans-now.shp timezones_import > /ressources/timezones_import.sql

echo Writing sql to database
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f /ressources/timezones_import.sql

echo Migrate data from import table to final table
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f /scripts/update_timezones.sql


echo --- Finished timezones import ---
