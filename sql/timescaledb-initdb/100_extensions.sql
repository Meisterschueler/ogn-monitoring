-- activate TimescaleDB, PostGIS and CRON
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- activate PostgreSQL performance logging
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- activate telemetry for TimescaleDB
ALTER DATABASE "ogn" SET timescaledb.telemetry_level = 'basic';

-- set tuple decompression limit to unlimited (==0)
SET timescaledb.max_tuples_decompressed_per_dml_transaction TO 0
