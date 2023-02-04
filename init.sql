CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

DROP TABLE IF EXISTS positions CASCADE;
CREATE TABLE IF NOT EXISTS positions (
    "ts"                TIMESTAMPTZ NOT NULL,
    src_call            CHAR(9) NOT NULL,
    dst_call            CHAR(9) NOT NULL,
    receiver            CHAR(9) NOT NULL,
    latitude            DOUBLE PRECISION NOT NULL,
    longitude           DOUBLE PRECISION NOT NULL,
    symbol_table        CHAR NOT NULL,
    symbol_code         CHAR NOT NULL,
    course              DOUBLE PRECISION,
    speed               DOUBLE PRECISION,
    altitude            DOUBLE PRECISION,
    address_type        SMALLINT,
    aircraft_type       SMALLINT,
    is_stealth          BOOLEAN,
    is_notrack          BOOLEAN,
    address	            INTEGER,
    climb_rate          DOUBLE PRECISION,
    turn_rate           DOUBLE PRECISION,
    error               SMALLINT,
    frequency_offset    DOUBLE PRECISION,
    signal_quality      DOUBLE PRECISION,
    gps_quality         TEXT,
    flight_level        DOUBLE PRECISION,
    signal_power        DOUBLE PRECISION,
    software_version    DOUBLE PRECISION,
    hardware_version    INTEGER,
    real_id             INTEGER,
    comment             TEXT,
    distance            DOUBLE PRECISION,
    normalized_quality  DOUBLE PRECISION
);

DROP TABLE IF EXISTS receivers;
CREATE TABLE IF NOT EXISTS receivers (
    id                  SERIAL,
    name                CHAR(9) NOT NULL UNIQUE,
    first_seen          TIMESTAMPTZ NOT NULL,
    last_seen           TIMESTAMPTZ NOT NULL,
    
    last_receiption     TIMESTAMPTZ,

    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    altitude            DOUBLE PRECISION
);

DROP TABLE IF EXISTS senders;
CREATE TABLE IF NOT EXISTS senders (
    id                  SERIAL,
    name                CHAR(9) NOT NULL UNIQUE,
    first_seen          TIMESTAMPTZ NOT NULL,
    last_seen           TIMESTAMPTZ NOT NULL,

    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    altitude            DOUBLE PRECISION,

    software_version    DOUBLE PRECISION,
    hardware_version    INTEGER
);

DROP TABLE IF EXISTS statistics;
CREATE TABLE IF NOT EXISTS statistics (
    "ts"                TIMESTAMPTZ NOT NULL,
    sender              CHAR(9) NOT NULL,
    receiver            CHAR(9) NOT NULL,
    points_good         INTEGER,
    points_total        INTEGER,
    distance            DOUBLE PRECISION,
    normalized_quality  DOUBLE PRECISION,

    UNIQUE (ts, sender, receiver)
);

DROP TABLE IF EXISTS sender_statistics;
CREATE TABLE IF NOT EXISTS sender_statistics (
    "ts"                TIMESTAMPTZ NOT NULL,
    sender              CHAR(9) NOT NULL,
    points_good         INTEGER,
    points_total        INTEGER,
    distance            DOUBLE PRECISION,
    normalized_quality  DOUBLE PRECISION,
    receiver_count      INTEGER,
    
    UNIQUE (ts, sender)
);

DROP TABLE IF EXISTS receiver_statistics;
CREATE TABLE IF NOT EXISTS receiver_statistics (
    "ts"                TIMESTAMPTZ NOT NULL,
    receiver            CHAR(9) NOT NULL,
    points_good         INTEGER,
    points_total        INTEGER,
    distance            DOUBLE PRECISION,
    normalized_quality  DOUBLE PRECISION,
    sender_count        INTEGER,
    
    UNIQUE (ts, receiver)
);


SELECT create_hypertable('positions', 'ts', chunk_time_interval => INTERVAL '6 hours');
SELECT create_hypertable('statistics', 'ts', chunk_time_interval => INTERVAL '1 day');
SELECT create_hypertable('sender_statistics', 'ts', chunk_time_interval => INTERVAL '10 days');
SELECT create_hypertable('receiver_statistics', 'ts', chunk_time_interval => INTERVAL '10 days');