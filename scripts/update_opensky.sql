CREATE TEMPORARY TABLE opensky_import (
	icao24	TEXT,
	registration	TEXT,
	manufacturericao	TEXT,
	manufacturername	TEXT,
	model		TEXT,
	typecode	TEXT,
	serialnumber	TEXT,
	linenumber	TEXT,
	icaoaircrafttype	TEXT,
	operator	TEXT,
	operatorcallsign	TEXT,
	operatoricao	TEXT,
	operatoriata	TEXT,
	owner		TEXT,
	testreg		TEXT,
	registered	TEXT,
	reguntil	TEXT,
	status		TEXT,
	built		TEXT,
	firstflightdate	TEXT,
	seatconfiguration	TEXT,
	engines		TEXT,
	modes		TEXT,
	adsb		TEXT,
	acars		TEXT,
	notes		TEXT,
	categoryDescription	TEXT
);
\copy opensky_import FROM '/ressources/opensky_aircraft_database.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"');

INSERT INTO opensky (address, registration, manufacturer, model)
SELECT
	('x' || lpad(icao24, 8, '0'))::bit(32)::int AS address,
	registration,
	manufacturername AS manufacturer,
	model
FROM opensky_import
ORDER BY address;
