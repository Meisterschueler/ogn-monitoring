TRUNCATE flarm_expiry;
\copy flarm_expiry FROM '/ressources/flarm_expiry.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"');

TRUNCATE flarm_hardware;
\copy flarm_hardware FROM '/ressources/flarm_hardware.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"');

CREATE TEMPORARY TABLE icao24bit_import (
	iso2	TEXT,
	country	TEXT,
	lower_limit	TEXT,
	upper_limit	TEXT
);
\copy icao24bit_import FROM '/ressources/iso2_icao24bit.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"');

TRUNCATE icao24bit;
INSERT INTO icao24bit
SELECT
	iso2,
	('x' || lpad(lower_limit, 8, '0'))::bit(32)::int,
	('x' || lpad(upper_limit, 8, '0'))::bit(32)::int
FROM icao24bit_import
WHERE iso2 != ''
ORDER BY lower_limit;

CREATE TEMPORARY TABLE registrations_import (
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
\copy registrations_import FROM '/ressources/iso2_registration_regex.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"');

TRUNCATE registrations;
INSERT INTO registrations(iso2, regex, description, aircraft_types)
SELECT
	iso2,
	regex,
	description,
	array_remove(ARRAY[unknown, glider, tow_plane, helicopter, parachute, drop_plane, hang_glider, para_glider, powered_aircraft, jet_aircraft, ufo, balloon, airship, uav, ground_support, static_object], NULL) AS aircraft_types FROM registrations_import
ORDER BY iso2, regex;

REFRESH MATERIALIZED VIEW ddb_joined;
