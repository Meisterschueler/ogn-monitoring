TRUNCATE flarm_expiry;
\copy flarm_expiry FROM './flarm_expiry.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"');

TRUNCATE flarm_hardware;
\copy flarm_hardware FROM './flarm_hardware.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"');

TRUNCATE icao24bit_import;
\copy icao24bit_import FROM './iso2_icao24bit.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"');

TRUNCATE icao24bit;
INSERT INTO icao24bit
SELECT
	iso2,
	('x' || lpad(lower_limit, 8, '0'))::bit(32)::int,
	('x' || lpad(upper_limit, 8, '0'))::bit(32)::int
FROM icao24bit_import
WHERE iso2 != 'NULL'
ORDER BY lower_limit;

TRUNCATE registrations_import;
\copy registrations_import FROM './iso2_registration_regex.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"');

TRUNCATE registrations;
INSERT INTO registrations(iso2, regex, description, aircraft_types)
SELECT
	iso2,
	regex,
	description,
	array_agg(array_remove(ARRAY[unknown, glider, tow_plane, helicopter, parachute, drop_plane, hang_glider, para_glider, powered_aircraft, jet_aircraft, ufo, balloon, airship, uav, ground_support, static_object], NULL)) AS aircraft_types
FROM registrations_import
GROUP BY iso2, regex, description
ORDER BY iso2, regex;

REFRESH MATERIALIZED VIEW ddb_registration;
