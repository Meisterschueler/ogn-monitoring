-- create tables we need for additional data
CREATE TABLE IF NOT EXISTS confirmations_1d (
    "ts"                TIMESTAMPTZ NOT NULL,
    receiver1           VARCHAR(9) NOT NULL,
    receiver2           VARCHAR(9) NOT NULL,
    altitude_delta      DOUBLE PRECISION,
	
    messages            INTEGER
);
CREATE UNIQUE INDEX confirmations_upsert_idx ON confirmations_1d (ts, receiver1, receiver2, altitude_delta);

CREATE TABLE IF NOT EXISTS records_1d (
    "ts"                TIMESTAMPTZ NOT NULL,
	src_call            VARCHAR(9) NOT NULL,
	receiver            VARCHAR(9) NOT NULL,

	ts_first            TIMESTAMPTZ NOT NULL,
	ts_last             TIMESTAMPTZ NOT NULL,
	distance_min        DOUBLE PRECISION,
	distance_max        DOUBLE PRECISION,
	altitude_min        DOUBLE PRECISION,
	altitude_max        DOUBLE PRECISION,
	normalized_quality_min  DOUBLE PRECISION,
	normalized_quality_max  DOUBLE PRECISION
);
CREATE UNIQUE INDEX records_upsert_idx ON records_1d(ts, src_call, receiver);
