CREATE TEMPORARY TABLE openaip_import (
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
\copy openaip_import FROM '/ressources/openaip.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"');

TRUNCATE openaip;
INSERT INTO openaip (name, code, iso2, location, altitude, style)
SELECT
	name,
	code,
	country AS iso2,
	ST_Point(
		(CAST(SUBSTRING(lon FOR 3) AS DOUBLE PRECISION) + CAST(SUBSTRING(lon FROM 4 FOR 6) AS DOUBLE PRECISION)/60.0) * CASE SUBSTRING(lon FROM 10 FOR 1) WHEN 'E' THEN 1 ELSE -1 END,
		(CAST(SUBSTRING(lat FOR 2) AS DOUBLE PRECISION) + CAST(SUBSTRING(lat FROM 3 FOR 6) AS DOUBLE PRECISION)/60.0) * CASE SUBSTRING(lat FROM 9 FOR 1) WHEN 'N' THEN 1 ELSE -1 END
	) AS location,
	CAST(SUBSTRING(elev FOR LENGTH(elev)-1) AS DOUBLE PRECISION) AS altitude,
	CAST(style AS SMALLINT) AS style
FROM openaip_import;
