-- create tables we need for external ressources
CREATE TABLE IF NOT EXISTS ddb (
    address             INTEGER NOT NULL UNIQUE,
    address_type        SMALLINT,
    model               VARCHAR(32),
    model_type          SMALLINT,
    registration        VARCHAR(7),
    cn                  VARCHAR(3),
    is_notrack          BOOLEAN NOT NULL,
    is_noident          BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS flarm_expiry (
    version             DOUBLE PRECISION NOT NULL UNIQUE,
    expiry_date         DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS flarm_hardware (
    id                  INTEGER PRIMARY KEY,
    code                TEXT NOT NULL,
    manufacturer        TEXT NOT NULL,
    model               TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS icao24bit (
    iso2                VARCHAR(2),
    lower_limit         INTEGER,
    upper_limit         INTEGER
);
CREATE INDEX idx_icao24bit_lower_limit_upper_limit ON icao24bit(lower_limit, upper_limit);

CREATE TABLE IF NOT EXISTS registrations (
    iso2                VARCHAR(2) NOT NULL,
    regex               TEXT NOT NULL,
    description         TEXT,
    aircraft_types      SMALLINT[]
);

CREATE TABLE IF NOT EXISTS openaip (
    name                TEXT,
    code                TEXT,
    iso2                VARCHAR(2),

    location            GEOMETRY(POINT, 4326),
    altitude            DOUBLE PRECISION,
    style               SMALLINT
);
CREATE INDEX idx_openaip_location ON openaip USING gist(location);

CREATE TABLE IF NOT EXISTS weglide (
    address             INTEGER PRIMARY KEY,
    registration        TEXT,
    CN                  TEXT,
    model               TEXT,
    until               TIMESTAMPTZ,
    pilot               TEXT
);

CREATE TABLE IF NOT EXISTS opensky (
    address             INTEGER PRIMARY KEY,
    registration        TEXT,
    manufacturer        TEXT,
    model               TEXT
);

CREATE TABLE IF NOT EXISTS flarmnet (
    address             INTEGER PRIMARY KEY,
    owner               TEXT,
    airport             TEXT,
    model               TEXT,
    registration        TEXT,
    CN                  TEXT,
    radio               TEXT
);
