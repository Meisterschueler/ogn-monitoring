CREATE TEMPORARY TABLE weglide_import (
	address		TEXT,
	registration	TEXT,
	cn			TEXT,
	model		TEXT,
	until		TEXT,
	pilot		TEXT
);
\copy weglide_import FROM './weglide.csv' WITH (FORMAT CSV, HEADER FALSE, QUOTE '''');

TRUNCATE weglide;
INSERT INTO weglide (address,registration,cn,model,until,pilot)
SELECT
	('x' || lpad(w.address, 8, '0'))::bit(32)::int AS address,
	w.registration,
	w.cn,
	w.model,
	w.until::TIMESTAMPTZ,
	w.pilot
FROM weglide_import AS w;

