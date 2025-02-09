#!/bin/bash

test ! -f /ressources/tiles/N35E129.hgt || exit 0

echo --- Started elevation import ---

echo Download tile files
mkdir -p /ressources/tiles
cat tiles.txt | xargs wget -P /ressources/tiles

echo Unzipping files
find /ressources/tiles/*.zip | xargs -n 1 unzip -j -d /ressources/tiles

echo Import elevation data into table 'elevations'
raster2pgsql -a -e -s 4326 -t 100x100 -Y /ressources/tiles/*.hgt elevations | PGPASSWORD=${POSTGRES_PASSWORD} psql --user=${POSTGRES_USER} --host=timescaledb --dbname=${POSTGRES_DB}

echo --- Finished elevation import ---
