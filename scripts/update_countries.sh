#!/bin/bash

echo --- Started country import ---

echo Download countries zip file
wget https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_0_countries.zip -O /ressources/ne_10m_admin_0_countries.zip

echo Unzipping archive
unzip /ressources/ne_10m_admin_0_countries.zip -o -d /ressources

echo Converting shp to sql
shp2pgsql -d -s 4326 -I -D /ressources/ne_10m_admin_0_countries.shp countries_import > /ressources/countries_import.sql

echo Writing sql to database
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f /ressources/countries_import.sql

echo Migrate data from import table to final table
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f /scripts/update_countries.sql


echo --- Finished country import ---
