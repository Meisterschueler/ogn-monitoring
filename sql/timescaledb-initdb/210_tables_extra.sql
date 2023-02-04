-- create tables we need for external ressources
CREATE TABLE ddb_import (
	DEVICE_TYPE 	TEXT,
	DEVICE_ID		TEXT,
	AIRCRAFT_MODEL	TEXT,
	REGISTRATION	TEXT,
	CN				TEXT,
	TRACKED			TEXT,
	IDENTIFIED		TEXT,
	AIRCRAFT_TYPE	TEXT
);

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

CREATE TABLE IF NOT EXISTS registrations_import (
	iso2			TEXT NOT NULL,
	regex			TEXT NOT NULL,
	description		TEXT,
	unknown			SMALLINT,
	glider			SMALLINT,
	tow_plane		SMALLINT,
	helicopter		SMALLINT,
	parachute		SMALLINT,
	drop_plane		SMALLINT,
	hang_glider		SMALLINT,
	para_glider		SMALLINT,
	powered_aircraft	SMALLINT,
	jet_aircraft	SMALLINT,
	ufo				SMALLINT,
	balloon			SMALLINT,
	airship			SMALLINT,
	uav				SMALLINT,
	ground_support	SMALLINT,
	static_object	SMALLINT
);

CREATE TABLE IF NOT EXISTS registrations (
	iso2			VARCHAR(2) NOT NULL,
	regex			TEXT NOT NULL,
	description		TEXT,
	aircraft_types	SMALLINT[]
);

CREATE TABLE IF NOT EXISTS openaip_import (
	name	TEXT,
	code	TEXT,
	country	TEXT,
	lat		TEXT,
	lon		TEXT,
	elev	TEXT,
	style	TEXT,
	rwdir	TEXT,
	rwlen	TEXT,
	rwwidth	TEXT,
	freq	TEXT,
	"desc"	TEXT
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

CREATE TABLE IF NOT EXISTS icao24bit_import (
	country		TEXT,
	lower_limit	TEXT,
	upper_limit	TEXT
);
