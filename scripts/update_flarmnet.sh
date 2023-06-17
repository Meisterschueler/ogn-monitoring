#!/bin/bash

echo --- Started Flarmnet import ---

echo Downloading flarmnet fln file
wget http://www.flarmnet.org/static/files/wfn/data.fln -O /ressources/data.fln

echo Write fln file to database
psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB} -f '/scripts/update_flarmnet.sql'

echo --- Finished Flarmnet import ---