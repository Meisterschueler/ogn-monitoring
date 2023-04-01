-- join the ddb with registrations - because "IS SIMILAR TO regex" is quite expensive
-- a refresh should be done only if ddb or registration is changed
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

	SUM(CASE WHEN d.registration != '' THEN 1 ELSE 0 END) OVER (PARTITION BY d.registration) AS double_registration,
	SUM(CASE WHEN d.registration != '' THEN 1 ELSE 0 END) OVER (PARTITION BY d.registration, d.address_type) AS double_registration_address_type,
	SUM(CASE WHEN d.registration != '' THEN 1 ELSE 0 END) OVER (PARTITION BY d.registration, d.cn, d.model) AS double_registration_cn_model
FROM ddb AS d
LEFT JOIN registrations AS r ON d.registration SIMILAR TO r.regex;
CREATE INDEX idx_ddb_joined_address ON ddb_joined (ddb_address);


-- create sender view with ALL relevant informations
CREATE MATERIALIZED VIEW senders_joined
AS
SELECT
	s.name AS sender_name,
	s.last_position AS sender_last_position,
	s.last_status AS sender_last_status,
	s.location AS sender_location,
	s.altitude AS sender_altitude,
	s.address_type AS sender_address_type,
	s.aircraft_type AS sender_aircraft_type,
	s.is_stealth AS sender_is_stealth,
	s.is_notrack AS sender_is_notrack,
	s.address AS sender_address,
	s.software_version AS sender_software_version,
	s.hardware_version AS sender_hardware_version,
	s.original_address AS sender_original_address,
	dj.*,
	fh.manufacturer AS flarm_hardware_manufacturer,
	fh.model AS flarm_hardware_model,
	fe.expiry_date AS flarm_expiry_date,
	a.name AS airport_name,
	a.code AS airport_code,
	a.iso2 AS airport_iso2,
	a.location AS airport_location,
	a.altitude AS airport_altitude,
	a.style AS airport_style,
	CASE
		WHEN s.location IS NOT NULL AND a.location IS NOT NULL AND ST_Distance(s.location, a.location) < 5
		THEN
			ST_Distance(
				ST_TRANSFORM(s.location, 3857),
				ST_TRANSFORM(a.location, 3857)
			)
		ELSE NULL
	END as airport_distance,
	degrees(ST_Azimuth(s.location, a.location)) AS airport_radial,
	o.registration AS opensky_registration,
	o.manufacturer AS opensky_manufacturer,
	o.model AS opensky_model,
	w.registration AS weglide_registration,
	w.cn AS weglide_cn,
	w.model AS weglide_model,
	w.until AS weglide_until,
	w.pilot AS weglide_pilot,
	i.iso2 AS icao24bit_iso2,
	i.lower_limit AS icao24bit_lower_limit,
	i.upper_limit AS icao24bit_upper_limit,
	CASE
		WHEN dj.registration_iso2 IS NOT NULL THEN dj.registration_iso2
		WHEN i.iso2 IS NOT NULL THEN i.iso2
		ELSE ''
	END AS iso2,
	CASE
		WHEN COALESCE(dj.ddb_registration, '') != '' THEN dj.ddb_registration
		WHEN COALESCE(o.registration, '') != '' THEN o.registration || ' (opensky)'
		WHEN COALESCE(w.registration, '') != '' THEN w.registration || ' (weglide)'
		ELSE ''
	END AS registration,
	CASE
		WHEN COALESCE(dj.ddb_model, '') != '' THEN dj.ddb_model
		WHEN COALESCE(o.model, '') != '' THEN o.model || ' (opensky)'
		WHEN COALESCE(w.model, '') != '' THEN w.model || ' (weglide)'
		ELSE ''
	END AS model,
	CASE 
        WHEN s.aircraft_type IS NULL OR dj.registration_aircraft_types IS NULL THEN ''
        WHEN s.aircraft_type = ANY(dj.registration_aircraft_types) THEN 'OK'
        WHEN 0 = ALL(dj.registration_aircraft_types) THEN 'GENERIC'
        ELSE 'ERROR'
    END AS check_registration_aircraft_types,
	CASE
		WHEN double_registration IS NULL THEN ''
		WHEN double_registration = 0 THEN ''
		WHEN double_registration = 1 THEN 'OK'
		WHEN double_registration = dj.double_registration_cn_model AND dj.double_registration_address_type = 1 THEN 'WARNING'
		ELSE 'ERROR'
	END AS check_registration_double,
	CASE
		WHEN 
			(s.name LIKE 'ICA%' OR s.name LIKE 'PAW%')
			AND dj.registration_iso2 IS NOT NULL
			AND i.iso2 IS NOT NULL
			AND dj.registration_iso2 != i.iso2
		THEN 'ERROR'
		WHEN 
			(s.name LIKE 'ICA%' OR s.name LIKE 'PAW%')
			AND dj.registration_iso2 IS NOT NULL
			AND i.iso2 IS NOT NULL
			AND dj.registration_iso2 = i.iso2
		THEN 'OK'
		ELSE ''
	END AS check_iso2,
	CASE
		WHEN dj.ddb_model_type IS NULL OR dj.ddb_model IS NULL THEN ''
		WHEN s.aircraft_type = 1 AND dj.ddb_model_type = 1 THEN 'OK'			-- (moto-)glider -> Gliders/motoGliders
		WHEN s.aircraft_type = 2 AND dj.ddb_model_type IN (1,2,3) THEN 'OK'		-- tow plane -> Gliders/motoGliders, Planes or Ultralight
		WHEN s.aircraft_type = 3 AND dj.ddb_model_type = 4 THEN 'OK'			-- helicopter -> Helicopter
		WHEN s.aircraft_type = 6 AND dj.ddb_model_type = 6 AND dj.ddb_model = 'HangGlider' THEN 'OK'  -- hang-glider -> Others::HangGlider
		WHEN s.aircraft_type = 7 AND dj.ddb_model_type = 6 AND dj.ddb_model = 'Paraglider' THEN 'OK'  -- para-glider -> Others::Paraglider
		WHEN s.aircraft_type = 8 AND dj.ddb_model_type IN (2,3) THEN 'OK'		-- powered aircraft -> Planes or Ultralight
		WHEN s.aircraft_type = 9 AND dj.ddb_model_type = 2 THEN 'OK'			-- jet aircraft -> Planes
		WHEN s.aircraft_type = 10 AND dj.ddb_model_type = 6 AND dj.ddb_model = 'UFO' THEN 'OK'		-- UFO -> Others::UFO
		WHEN s.aircraft_type = 11 AND dj.ddb_model_type = 6 AND dj.ddb_model = 'Balloon' THEN 'OK'	-- Balloon -> Others::Balloon
		WHEN s.aircraft_type = 13 AND dj.ddb_model_type = 5 THEN 'OK'			-- UAV -> Drones/UAV
		WHEN s.aircraft_type = 14 AND dj.ddb_model_type = 6 AND dj.ddb_model = 'Ground Station' THEN 'OK'	-- ground support -> Others::Ground Station
		WHEN s.aircraft_type = 15 AND dj.ddb_model_type = 6 AND dj.ddb_model = 'Ground Station' THEN 'OK'	-- static object -> Others::Ground Station
		WHEN s.aircraft_type IS NULL THEN 'UNKNOWN'
		ELSE 'ERROR'
	END AS check_model_type,
	CASE
        WHEN s.address_type IS NULL OR dj.ddb_address_type IS NULL THEN 'UNKNOWN'
        WHEN s.address_type != dj.ddb_address_type THEN 'ERROR'
        ELSE 'OK'
    END AS check_address_type,
	CASE
		WHEN fe.expiry_date IS NULL THEN ''
		WHEN fe.expiry_date - NOW() > INTERVAL'1 year' THEN 'OK'
		WHEN fe.expiry_date - NOW() > INTERVAL'2 months' THEN 'WARNING'
		WHEN fe.expiry_date - NOW() > INTERVAL'1 day' THEN 'DANGER'
		ELSE 'EXPIRED'
	END AS check_flarm_expiry_date,
	CASE
		WHEN dj.ddb_registration IS NULL OR dj.ddb_registration = '' OR o.registration IS NULL OR o.registration = '' THEN ''
		WHEN dj.ddb_registration IS NOT NULL AND o.registration IS NOT NULL AND dj.ddb_registration = o.registration THEN 'OK'
		ELSE 'ERROR'
	END AS check_opensky_registration,
	CASE
		WHEN dj.ddb_registration IS NULL OR w.registration IS NULL THEN ''
		WHEN dj.ddb_registration IS NOT NULL AND w.registration IS NOT NULL AND dj.ddb_registration = w.registration THEN 'OK'
		ELSE 'ERROR'
	END AS check_weglide_registration
FROM senders AS s
LEFT JOIN ddb_joined AS dj ON s.address = dj.ddb_address
LEFT JOIN flarm_hardware AS fh ON s.hardware_version = fh.id
LEFT JOIN flarm_expiry AS fe ON s.software_version = fe.version
LEFT JOIN opensky AS o ON s.address = o.address
LEFT JOIN weglide AS w ON s.address = w.address
LEFT JOIN icao24bit AS i ON s.address BETWEEN lower_limit AND upper_limit
CROSS JOIN LATERAL (
	SELECT *
	FROM openaip
	ORDER BY openaip.location <-> s.location
	LIMIT 1
) AS a;
CREATE INDEX idx_senders_joined_airport_iso2_airport_name ON senders_joined (airport_iso2, airport_name);

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
FROM opensky AS o;

CREATE MATERIALIZED VIEW senders_relative_qualities
AS
SELECT
	time_bucket('1 day', rs.ts) AS ts,
	rs.src_call,
	
	AVG(rs.normalized_quality - sq.normalized_quality) AS relative_quality,
	SUM(CASE WHEN rs.normalized_quality < sq.percentile_10 THEN 1 ELSE 0 END) AS reporting_below_10,
	COUNT(*) AS total
FROM ranking_statistics_1d AS rs
INNER JOIN (
	SELECT
		time_bucket('1 day', ts) AS ts,	
		receiver,

		AVG(normalized_quality) AS normalized_quality,
		COUNT(DISTINCT src_call) AS senders_count,
		percentile_disc(0.1) WITHIN GROUP (order by normalized_quality) AS percentile_10,
		percentile_disc(0.2) WITHIN GROUP (order by normalized_quality) AS percentile_20,
		percentile_disc(0.3) WITHIN GROUP (order by normalized_quality) AS percentile_30,
		percentile_disc(0.4) WITHIN GROUP (order by normalized_quality) AS percentile_40,
		percentile_disc(0.5) WITHIN GROUP (order by normalized_quality) AS percentile_50,
		percentile_disc(0.6) WITHIN GROUP (order by normalized_quality) AS percentile_60,
		percentile_disc(0.7) WITHIN GROUP (order by normalized_quality) AS percentile_70,
		percentile_disc(0.8) WITHIN GROUP (order by normalized_quality) AS percentile_80,
		percentile_disc(0.9) WITHIN GROUP (order by normalized_quality) AS percentile_90
	FROM ranking_statistics_1d
	WHERE normalized_quality IS NOT NULL AND points_motion > 10
	GROUP BY time_bucket('1 day', ts), receiver
) AS sq ON sq.ts = rs.ts AND sq.receiver = rs.receiver
GROUP BY time_bucket('1 day', rs.ts), rs.src_call

UNION

SELECT
  'WeGlide' AS "source",
  w.address AS "address",
  w.registration AS "registration",
  w.model AS "model"
FROM weglide AS w;
CREATE INDEX ON registration_joined(address, registration);
CREATE INDEX ON registration_joined(registration, address);

-- Create normalized sender qualities
CREATE MATERIALIZED VIEW senders_relative_qualities
AS
SELECT
	time_bucket('1 day', rs.ts) AS ts,
	rs.src_call,
	
	AVG(rs.normalized_quality - sq.normalized_quality) AS relative_quality,
	SUM(CASE WHEN rs.normalized_quality < sq.percentile_10 THEN 1 ELSE 0 END) AS reporting_below_10,
	COUNT(*) AS total
FROM ranking_statistics_1d AS rs
INNER JOIN (
	SELECT
		time_bucket('1 day', ts) AS ts,	
		receiver,

		AVG(normalized_quality) AS normalized_quality,
		COUNT(DISTINCT src_call) AS senders_count,
		percentile_disc(0.1) WITHIN GROUP (order by normalized_quality) AS percentile_10,
		percentile_disc(0.2) WITHIN GROUP (order by normalized_quality) AS percentile_20,
		percentile_disc(0.3) WITHIN GROUP (order by normalized_quality) AS percentile_30,
		percentile_disc(0.4) WITHIN GROUP (order by normalized_quality) AS percentile_40,
		percentile_disc(0.5) WITHIN GROUP (order by normalized_quality) AS percentile_50,
		percentile_disc(0.6) WITHIN GROUP (order by normalized_quality) AS percentile_60,
		percentile_disc(0.7) WITHIN GROUP (order by normalized_quality) AS percentile_70,
		percentile_disc(0.8) WITHIN GROUP (order by normalized_quality) AS percentile_80,
		percentile_disc(0.9) WITHIN GROUP (order by normalized_quality) AS percentile_90
	FROM ranking_statistics_1d
	WHERE normalized_quality IS NOT NULL AND points_motion > 10
	GROUP BY time_bucket('1 day', ts), receiver
) AS sq ON sq.ts = rs.ts AND sq.receiver = rs.receiver
GROUP BY time_bucket('1 day', rs.ts), rs.src_call
