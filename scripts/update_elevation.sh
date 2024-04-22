#!/bin/bash

echo --- Started elevation import ---

echo Download tile files
cat tiles.txt | xargs wget -P /tiles

echo Unzipping files
find /tiles/*.zip | xargs -n 1 unzip -j -d /tiles

echo Import elevation data into table 'elevations'
raster2pgsql -a -e -s 4326 -t 100x100 -Y /tiles/*.hgt elevations | PGPASSWORD=${POSTGRES_PASSWORD} psql --user=${POSTGRES_USER} --host=timescaledb --dbname=${POSTGRES_DB}

echo --- Finished elevation import ---