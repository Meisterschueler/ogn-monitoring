-- join the ddb with registrations
-- cost: 15s (because "IS SIMILAR TO regex")
CREATE MATERIALIZED VIEW ddb_joined
AS
SELECT
    sq.*,
	CASE
		WHEN sq.ddb_registration ~ '([A-Z0-9]{1,2}\-[A-Z0-9]{3,4}|N[A-Z0-9]{1,5})' IS FALSE THEN 'ERROR'
		WHEN sq.registration_iso2 IS NULL THEN 'WARNING'
		ELSE 'OK'
	END AS check_registration_valid,
	CASE
		WHEN sq.registration_iso2 IS NULL OR sq.icao24bit_iso2 IS NULL THEN ''
		WHEN sq.registration_iso2 = sq.icao24bit_iso2 THEN 'OK'
		ELSE 'ERROR'
	END AS check_registration_icao24bit_iso2,
	CASE
		WHEN sq.ddb_aircraft_types IS NULL OR sq.registration_aircraft_types IS NULL THEN ''
		WHEN sq.ddb_aircraft_types::smallint[] && sq.registration_aircraft_types THEN 'OK'
		WHEN sq.registration_aircraft_types = ARRAY[0]::smallint[] THEN 'GENERIC'
		ELSE 'ERROR'
	END AS check_ddb_registration_aircraft_type
FROM (
	SELECT
		d.address AS ddb_address,
		d.address_type AS ddb_address_type,
		d.model AS ddb_model,
		d.model_type AS ddb_model_type,
		d.registration AS ddb_registration,
		d.cn AS ddb_cn,
		d.is_notrack AS ddb_is_notrack,
		d.is_noident AS ddb_is_noident,
		d.registration_addresses,
		r.iso2 AS registration_iso2,
		r.regex AS registration_regex,
		r.description AS registration_description,
		r.aircraft_types AS registration_aircraft_types,
		i.iso2 AS icao24bit_iso2,
		i.lower_limit AS icao24bit_lower_limit,
		i.upper_limit AS icao24bit_upper_limit,

		-- ddb: 1 = GLIDER/MOTORGLIDER, 2 = PLANE, 3 = ULTRALIGHT, 4 = HELICOPTER, 5 = DRONE, 6 = OTHER
		-- flarm: 1 = GLIDER/MOTORGLIDER, 2 = TOWPLANE, 3 = HELICOPTER, 4 = PARACHUTE, 5 = DROPPLANE,
		-- ...    6 = HANGGLIDER, 7 = PARAGLIDER, 8 = PLANE, 9 = JET, 10 = UFO, 11 = BALLOON, 12 = AIRSHIP,
		-- ...   13 = UAV, 14 = GROUND SUPPORT, 15 = STATIC OBJECT
		CASE
			WHEN d.model_type = 1 THEN ARRAY[1, 2]
			WHEN d.model_type = 2 AND d.model = 'Towplane' THEN ARRAY[2]
			WHEN d.model_type = 2 THEN ARRAY[2, 8, 9]
			WHEN d.model_type = 3 THEN ARRAY[2, 8]
			WHEN d.model_type = 4 THEN ARRAY[3]
			WHEN d.model_type = 5 THEN ARRAY[13]
			WHEN d.model_type = 6 AND d.model = 'HangGlider' THEN ARRAY[6]
			WHEN d.model_type = 6 AND d.model = 'Paraglider' THEN ARRAY[7]
			WHEN d.model_type = 6 AND d.model = 'UFO' THEN ARRAY[10]
			WHEN d.model_type = 6 AND d.model = 'Balloon' THEN ARRAY[11]
			WHEN d.model_type = 6 AND d.model = 'Ground Station' THEN ARRAY[14, 15]
			WHEN d.model_type = 6 AND d.model = 'Unknown' THEN ARRAY[0]
			ELSE NULL::integer[]
		END AS ddb_aircraft_types
	FROM (
		SELECT
			ddb.*,
			sa.addresses AS registration_addresses
		FROM ddb
		LEFT JOIN (
			SELECT
				registration,
				ARRAY_AGG(address) AS addresses
			FROM ddb
			GROUP BY 1
		) AS sa ON ddb.registration = sa.registration
	) AS d
	LEFT JOIN registrations r ON d.registration ~ similar_to_escape(r.regex)
	LEFT JOIN icao24bit i ON d.address >= i.lower_limit AND d.address <= i.upper_limit
) AS sq;
CREATE UNIQUE INDEX idx_ddb_joined_address ON ddb_joined (ddb_address);

-- cost: 2s
CREATE MATERIALIZED VIEW registration_joined
AS
SELECT
  'DDB' AS "source",
  dj.ddb_address AS "address",
  dj.ddb_registration AS "registration",
  dj.ddb_cn AS "cn",
  dj.ddb_model AS "model"
FROM ddb_joined AS dj

UNION

SELECT
  'OpenSky' AS "source",
  o.address AS "address",
  o.registration AS "registration",
  NULL AS "cn",
  o.model AS "model"
FROM opensky AS o

UNION

SELECT
  'WeGlide' AS "source",
  w.address AS "address",
  w.registration AS "registration",
  w.cn AS "cn",
  w.model AS "model"
FROM weglide AS w

UNION

SELECT
  'Flarmnet' AS "source",
  f.address AS "address",
  f.registration AS "registration",
  CASE WHEN f.cn = '' THEN NULL ELSE f.cn END AS "cn",
  f.model AS "model"
FROM flarmnet AS f;
CREATE UNIQUE INDEX registration_joined_idx ON registration_joined(source, address);
CREATE INDEX ON registration_joined(address, registration);
CREATE INDEX ON registration_joined(registration, address);
