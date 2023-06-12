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
	testreg		TEXT
);
\copy opensky_import FROM '/ressources/opensky_aircraft_database.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"', ENCODING 'windows-1251');

TRUNCATE opensky;
INSERT INTO opensky (address, registration, manufacturer, model)
SELECT address, registration, manufacturer, model
FROM (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY address) AS row
	FROM (
		SELECT
			('x' || lpad(icao24, 8, '0'))::bit(32)::int AS address,
			registration,
			manufacturername AS manufacturer,
			model
		FROM opensky_import
	) AS sq
) AS sq2
WHERE sq2.row = 1
ORDER BY address;
