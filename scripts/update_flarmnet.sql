CREATE TEMPORARY TABLE IF NOT EXISTS flarmnet_import (
    raw    TEXT
);

\copy flarmnet_import FROM '/ressources/data.fln';

TRUNCATE flarmnet;
INSERT INTO flarmnet (address, owner, airport, model, registration, CN, radio)
SELECT
	('x' || lpad(substr(sq.raw, 0, 7), 8, '0'))::bit(32)::int AS address,
	trim(substr(sq.raw, 7, 21)) AS owner,
	trim(substr(sq.raw, 28, 21)) AS airport,
	trim(substr(sq.raw, 49, 21)) AS model,
	trim(substr(sq.raw, 70, 7)) AS registration,
	trim(substr(sq.raw, 77, 3)) AS CN,
	trim(substr(sq.raw, 80, 7)) AS radio
FROM (
	SELECT
		hex_to_string(raw) AS raw
	FROM flarmnet_import
	WHERE length(raw) = 172
	GROUP BY 1
) AS sq;
