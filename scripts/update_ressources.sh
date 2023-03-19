#!/bin/bash

echo Download ressources
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitor/master/ressources/flarm_expiry.csv -O /ressources/flarm_expiry.csv
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitor/master/ressources/flarm_hardware.csv -O /ressources/flarm_hardware.csv
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitor/master/ressources/iso2_icao24bit.csv -O /ressources/iso2_icao24bit.csv
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitor/master/ressources/iso2_registration_regex.csv -O /ressources/iso2_registration_regex.csv

echo Write ressources to database
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f /scripts/update_ressources.sql

echo Finished ressources import