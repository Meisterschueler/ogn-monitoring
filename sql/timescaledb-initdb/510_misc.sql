-- join the ddb with registrations
-- cost: 15s (because "IS SIMILAR TO regex")
CREATE MATERIALIZED VIEW ddb_joined
AS
SELECT
	d.address AS ddb_address,
	d.address_type AS ddb_address_type,
	d.model AS ddb_model,
	d.model_type AS ddb_model_type,
	d.registration AS ddb_registration,
	d.cn AS ddb_cn,
	d.is_notrack AS ddb_is_notrack,
	d.is_noident AS ddb_is_noident,
	r.iso2 AS registration_iso2,
	r.regex AS registration_regex,
	r.description AS registration_description,
	r.aircraft_types AS registration_aircraft_types,

	CASE
		-- no registration found
		WHEN d.registration_count = 0 THEN ''
	
		-- registration is unique
		WHEN d.registration_count = 1 THEN 'OK'

		-- registration + same CN + same model is seen twice but address type is different (can be ICAO address and default ID from same flarm)
		WHEN d.registration_count = 2 AND d.registration_cn_model_count = 2 AND d.registration_address_type_count = 1 THEN 'WARNING'

		ELSE 'ERROR'
	END as check_registration_unique
FROM (
	SELECT
		*,

		SUM(CASE WHEN registration != '' THEN 1 ELSE 0 END) OVER (PARTITION BY registration) AS registration_count,
		SUM(CASE WHEN registration != '' THEN 1 ELSE 0 END) OVER (PARTITION BY registration, address_type) AS registration_address_type_count,
		SUM(CASE WHEN registration != '' THEN 1 ELSE 0 END) OVER (PARTITION BY registration, cn, model) AS registration_cn_model_count
	FROM ddb
) AS d
LEFT JOIN registrations AS r ON d.registration SIMILAR TO r.regex;
CREATE UNIQUE INDEX idx_ddb_joined_address ON ddb_joined (ddb_address);

-- cost: 2s
CREATE MATERIALIZED VIEW registration_joined
AS
SELECT
  'DDB' AS "source",
  dj.ddb_address AS "address",
  dj.ddb_registration AS "registration",
  dj.ddb_model AS "model"
FROM ddb_joined AS dj

UNION

SELECT
  'OpenSky' AS "source",
  o.address AS "address",
  o.registration AS "registration",
  o.model AS "model"
FROM opensky AS o

UNION

SELECT
  'WeGlide' AS "source",
  w.address AS "address",
  w.registration AS "registration",
  w.model AS "model"
FROM weglide AS w

UNION

SELECT
  'Flarmnet' AS "source",
  f.address AS "address",
  f.registration AS "registration",
  f.model AS "model"
FROM flarmnet AS f;
CREATE UNIQUE INDEX registration_joined_idx ON registration_joined(source, address);
CREATE INDEX ON registration_joined(address, registration);
CREATE INDEX ON registration_joined(registration, address);
