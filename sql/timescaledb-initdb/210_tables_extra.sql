-- create tables we need for external ressources
CREATE TABLE IF NOT EXISTS ddb (
	address			INTEGER NOT NULL UNIQUE,
	address_type 	SMALLINT,
	model			VARCHAR(25),
	model_type		SMALLINT,
	registration	VARCHAR(7),
	cn				VARCHAR(3),
	is_notrack		BOOLEAN NOT NULL,
	is_noident		BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS flarm_hardware (
    id              TEXT NOT NULL UNIQUE,
    manufacturer    TEXT NOT NULL,
    model           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS flarm_expiry (
    version         DOUBLE PRECISION NOT NULL UNIQUE,
    expiry_date     DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS registrations (
	iso2			VARCHAR(2) NOT NULL,
	regex			TEXT NOT NULL,
	description		TEXT,
	aircraft_types	SMALLINT[]
);

CREATE TABLE IF NOT EXISTS openaip (
	name	TEXT,
	code	TEXT,
	iso2	VARCHAR(2),

	location	GEOMETRY(POINT, 4326),
	altitude	DOUBLE PRECISION,
	style	SMALLINT
);
CREATE INDEX idx_openaip_location ON openaip USING gist(location);

CREATE TABLE IF NOT EXISTS weglide (
	address	INTEGER,
	registration	TEXT,
	CN	TEXT,
	model	TEXT,
	until	TIMESTAMPTZ,
	pilot	TEXT
);

CREATE TABLE IF NOT EXISTS opensky (
	address	INTEGER,
	registration	TEXT,
	manufacturer	TEXT,
	model	TEXT
);
