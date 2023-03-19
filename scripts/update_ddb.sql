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

\copy ddb_import FROM './ddb.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '''');

TRUNCATE ddb;
INSERT INTO ddb (address,address_type,model,model_type,registration,cn,is_notrack,is_noident)
SELECT
	('x' || lpad(di.device_id, 8, '0'))::bit(32)::int AS address,
	CASE di.device_type
		WHEN 'I' THEN 1
		WHEN 'F' THEN 2
		WHEN 'O' THEN 3
	END AS address_type,
	di.aircraft_model AS model,
	CAST(di.aircraft_type AS SMALLINT) AS model_type,
	di.registration,
	di.cn,
	di.tracked = 'N' AS is_notrack,
	di.identified = 'N' AS is_noident
FROM ddb_import AS di;

REFRESH MATERIALIZED VIEW ddb_registration;
