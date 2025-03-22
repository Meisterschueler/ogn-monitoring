CREATE TEMPORARY TABLE vereinsflieger_import (
	"FlarmId"	TEXT,
	"Lfz."	TEXT,
	"Wkz"	TEXT,
	"Luftfahrzeugart"	TEXT,
	"Musterbezeichnung"		TEXT,
	"Eigentümer/Halter"	TEXT,
	"Vereins-LFZ"	TEXT
);
\copy vereinsflieger_import FROM '/ressources/vereinsflieger.csv' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"', ENCODING 'windows-1251');

TRUNCATE vereinsflieger;
INSERT INTO vereinsflieger (address, registration, CN, aircraft_type, model, owner, is_club, is_duplicate)
SELECT
	sq.address, sq.registration, sq.CN, sq.aircraft_type, sq.model, sq.owner, sq.is_club, sq.is_duplicate
FROM (
	SELECT
		('x' || lpad("FlarmId", 8, '0'))::bit(32)::int AS address,
		"Lfz." AS registration,
		"Wkz" AS CN,
		CASE "Luftfahrzeugart"
			WHEN 'Segelflugzeug' THEN 1
			ELSE 0
		END	AS aircraft_type,
		"Musterbezeichnung" AS model,
		"Eigentümer/Halter" AS owner,
		CASE "Vereins-LFZ" WHEN 'Ja' THEN TRUE ELSE FALSE END AS is_club,
		COUNT("FlarmId") OVER (PARTITION BY "FlarmId") > 1 AS is_duplicate,
		ROW_NUMBER() OVER (PARTITION BY "FlarmId") AS row
	FROM vereinsflieger_import
	WHERE LENGTH("FlarmId") = 6 AND "FlarmId" NOT LIKE '%[^0-9A-F]%'
) AS sq
WHERE sq.row = 1
ORDER BY address;
