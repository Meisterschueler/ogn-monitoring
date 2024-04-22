CREATE TABLE IF NOT EXISTS invalids (
    "ts"                TIMESTAMPTZ NOT NULL,
    raw_message         TEXT,
    error_message       TEXT
);

CREATE TABLE IF NOT EXISTS unknowns (
    "ts"                TIMESTAMPTZ NOT NULL,
    raw_message         TEXT,

    -- included in APRS message
    src_call            VARCHAR(9) NOT NULL,
    dst_call            VARCHAR(9) NOT NULL,
    receiver            VARCHAR(9) NOT NULL,
    comment             TEXT
);

CREATE TABLE IF NOT EXISTS positions (
    "ts"                TIMESTAMPTZ NOT NULL,

    -- APRS message body
    src_call            VARCHAR(9) NOT NULL,
    dst_call            VARCHAR(9) NOT NULL,
    receiver            VARCHAR(9) NOT NULL,

    -- APRS position message
    receiver_time       VARCHAR(7) NOT NULL,
    symbol_table        CHAR NOT NULL,
    symbol_code         CHAR NOT NULL,

    -- parsed APRS position comment
    course              SMALLINT,
    speed               SMALLINT,
    altitude            INTEGER,
    address_type        SMALLINT,
    aircraft_type       SMALLINT,
    is_stealth          BOOLEAN,
    is_notrack          BOOLEAN,
    address             INTEGER,
    climb_rate          INTEGER,
    turn_rate           DOUBLE PRECISION,
    error               SMALLINT,
    frequency_offset    DOUBLE PRECISION,
    signal_quality      DOUBLE PRECISION,
    gps_quality         TEXT,
    flight_level        DOUBLE PRECISION,
    signal_power        DOUBLE PRECISION,
    software_version    DOUBLE PRECISION,
    hardware_version    SMALLINT,
    original_address    INTEGER,

    unparsed            TEXT,

    -- additional (externally calculated) fields
    receiver_ts         TIMESTAMPTZ,
    bearing             DOUBLE PRECISION,
    distance            DOUBLE PRECISION,
    normalized_quality  DOUBLE PRECISION,

    -- additional (externally calculated) field, for PostGIS only
    location            GEOMETRY(POINT, 4326),
    elevation           INTEGER,

    -- bit coded plausibility check
    plausibility        SMALLINT
);
CREATE INDEX idx_positions_src_call ON positions (src_call, ts);

CREATE TABLE IF NOT EXISTS statuses (
    "ts"                TIMESTAMPTZ NOT NULL,

    -- APRS message body
    src_call            VARCHAR(9) NOT NULL,
    dst_call            VARCHAR(9) NOT NULL,
    receiver            VARCHAR(9) NOT NULL,

	-- APRS status message
    receiver_time       VARCHAR(7) NOT NULL,

	-- parsed APRS status comment
    version             TEXT,
    platform            TEXT,
    cpu_load            DOUBLE PRECISION,
    ram_free            DOUBLE PRECISION,
    ram_total           DOUBLE PRECISION,
    ntp_offset	        DOUBLE PRECISION,
    ntp_correction      DOUBLE PRECISION,
    voltage             DOUBLE PRECISION,
    amperage            DOUBLE PRECISION,
    cpu_temperature     DOUBLE PRECISION,
    visible_senders     SMALLINT,
    latency             DOUBLE PRECISION,
    senders             SMALLINT,
    rf_correction_manual        SMALLINT,
    rf_correction_automatic     DOUBLE PRECISION,
    noise                       DOUBLE PRECISION,
    senders_signal_quality      DOUBLE PRECISION,
    senders_messages            INTEGER,
    good_senders_signal_quality DOUBLE PRECISION,
    good_senders                INTEGER,
    good_and_bad_senders        INTEGER,

    unparsed            TEXT,

    -- additional (externally calculated) fields
    receiver_ts         TIMESTAMPTZ
);
CREATE INDEX idx_statuses_src_call ON statuses (src_call, ts);

CREATE TABLE IF NOT EXISTS events_receiver_status (
    "ts"                TIMESTAMPTZ NOT NULL,

    src_call            VARCHAR(9) NOT NULL,
    receiver            VARCHAR(9) NOT NULL,
    version             TEXT,
    platform            TEXT,
    senders_messages    INTEGER,

    event               SMALLINT
);
CREATE UNIQUE INDEX idx_events_receiver_status_src_call ON events_receiver_status (src_call, ts);

CREATE TABLE IF NOT EXISTS events_receiver_position (
    "ts"                TIMESTAMPTZ NOT NULL,

    src_call            VARCHAR(9) NOT NULL,
    altitude            INTEGER,
    location            GEOMETRY(POINT, 4326),

    event               SMALLINT
);
CREATE UNIQUE INDEX idx_events_receiver_position_src_call ON events_receiver_position (src_call, ts);

CREATE TABLE IF NOT EXISTS events_sender_position (
    "ts"                TIMESTAMPTZ NOT NULL,

    src_call            VARCHAR(9) NOT NULL,
    address_type        SMALLINT,
    aircraft_type       SMALLINT,
    is_stealth          BOOLEAN,
    is_notrack          BOOLEAN,
    address             INTEGER,
    software_version    DOUBLE PRECISION,
    hardware_version    SMALLINT,
    original_address    INTEGER,

    event               SMALLINT
);
CREATE UNIQUE INDEX idx_events_sender_position_src_call ON events_sender_position (src_call, ts);

CREATE TABLE IF NOT EXISTS events_takeoff (
    receiver_ts         TIMESTAMPTZ NOT NULL,

    src_call            VARCHAR(9) NOT NULL,
    course              SMALLINT,
    altitude            INTEGER,
    location            GEOMETRY(POINT, 4326),
    event               SMALLINT
);
CREATE UNIQUE INDEX idx_events_takeoff_src_call ON events_takeoff (src_call, receiver_ts);

CREATE TABLE IF NOT EXISTS takeoffs (
    receiver_ts         TIMESTAMPTZ NOT NULL,

    src_call            VARCHAR(9) NOT NULL,
    course              SMALLINT,
    event               SMALLINT,

    airport_name        TEXT,
    airport_iso2        VARCHAR(2),
    airport_tzid        VARCHAR(80)
);
CREATE UNIQUE INDEX idx_takeoffs_src_call ON takeoffs (src_call, receiver_ts);


-- create hypertables for messages tables
SELECT create_hypertable('invalids', 'ts', chunk_time_interval => INTERVAL '10 days');
SELECT create_hypertable('unknowns', 'ts', chunk_time_interval => INTERVAL '10 days');
SELECT create_hypertable('positions', 'ts', chunk_time_interval => INTERVAL '1 hour');
SELECT create_hypertable('statuses', 'ts', chunk_time_interval => INTERVAL '10 days');
SELECT create_hypertable('events_receiver_status', 'ts', chunk_time_interval => INTERVAL '10 days');
SELECT create_hypertable('events_receiver_position', 'ts', chunk_time_interval => INTERVAL '10 days');
SELECT create_hypertable('events_sender_position', 'ts', chunk_time_interval => INTERVAL '10 days');
SELECT create_hypertable('events_takeoff', 'receiver_ts', chunk_time_interval => INTERVAL '10 days');
SELECT create_hypertable('takeoffs', 'receiver_ts', chunk_time_interval => INTERVAL '10 days');