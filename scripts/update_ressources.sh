#!/bin/bash

echo --- Started ressources import ---

echo Download ressource csv files from github
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitoring/master/ressources/flarm_expiry.csv -O /ressources/flarm_expiry.csv
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitoring/master/ressources/flarm_hardware.csv -O /ressources/flarm_hardware.csv
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitoring/master/ressources/iso2_icao24bit.csv -O /ressources/iso2_icao24bit.csv
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitoring/master/ressources/iso2_registration_regex.csv -O /ressources/iso2_registration_regex.csv
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitoring/master/ressources/receiver_setup.csv -O /ressources/receiver_setup.csv

echo Write csv files to database
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f /scripts/update_ressources.sql

echo --- Finished ressources import ---
