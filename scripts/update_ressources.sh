echo Download ressources
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitor/master/ressources/flarm_expiry.csv
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitor/master/ressources/flarm_hardware.csv
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitor/master/ressources/iso2_icao24bit.csv
wget https://raw.githubusercontent.com/Meisterschueler/ogn-monitor/master/ressources/iso2_registration_regex.csv

echo Write to database
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f update_ressources.sql

echo Finished!
while true; do sleep 60; echo "sleeping"; done