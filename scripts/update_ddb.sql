CREATE TEMPORARY TABLE ddb_import (
	DEVICE_TYPE 	TEXT,
	DEVICE_ID		TEXT,
	AIRCRAFT_MODEL	TEXT,
	REGISTRATION	TEXT,
	CN				TEXT,
	TRACKED			TEXT,
	IDENTIFIED		TEXT,
	AIRCRAFT_TYPE	TEXT
);

\copy ddb_import FROM '/ressources/ddb.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '''');

TRUNCATE ddb;
INSERT INTO ddb (address,address_type,model,model_type,registration,cn,is_notrack,is_noident)
SELECT
	('x' || lpad(DEVICE_ID, 8, '0'))::bit(32)::int AS address,
	CASE DEVICE_TYPE
		WHEN 'I' THEN 1
		WHEN 'F' THEN 2
		WHEN 'O' THEN 3
	END AS address_type,
	AIRCRAFT_MODEL AS model,
	CAST(AIRCRAFT_TYPE AS SMALLINT) AS model_type,
	REGISTRATION AS registration,
	CN AS cn,
	TRACKED = 'N' AS is_notrack,
	IDENTIFIED = 'N' AS is_noident
FROM ddb_import AS di;

REFRESH MATERIALIZED VIEW ddb_joined;
