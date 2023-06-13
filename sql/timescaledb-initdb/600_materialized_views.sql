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
	s.is_duplicate AS sender_is_duplicate,
	s.messages AS sender_messages,
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
		WHEN s.is_duplicate THEN 'ERROR'
		ELSE 'OK'
	END AS check_sender_duplicate,
	CASE
		WHEN s.software_version != NULL AND fe.expiry_date IS NULL THEN 'ERROR'
		ELSE 'OK'
	END AS check_sender_software_version_plausible, 
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
FROM (
	SELECT
		*
	FROM (
		SELECT
			LAST(name, last_position) OVER (PARTITION BY name ORDER BY last_position) AS name,
			LAST(last_position, last_position) OVER (PARTITION BY name ORDER BY last_position) AS last_position,
			LAST(last_status, last_status) OVER (PARTITION BY name ORDER BY last_status) AS last_status,
			LAST(location, last_position) OVER (PARTITION BY name ORDER BY last_position) AS location,
			LAST(altitude, last_position) OVER (PARTITION BY name ORDER BY last_position) AS altitude,
			LAST(address_type, last_position) OVER (PARTITION BY name ORDER BY last_position) AS address_type,
			LAST(aircraft_type, last_position) OVER (PARTITION BY name ORDER BY last_position) AS aircraft_type,
			LAST(is_stealth, last_position) OVER (PARTITION BY name ORDER BY last_position) AS is_stealth,
			LAST(is_notrack, last_position) OVER (PARTITION BY name ORDER BY last_position) AS is_notrack,
			LAST(address, last_position) OVER (PARTITION BY name ORDER BY last_position) AS address,
			LAST(software_version, row) OVER (PARTITION BY name ORDER BY row) AS software_version,
			LAST(original_address, row) OVER (PARTITION BY name ORDER BY row) AS original_address,
			LAST(messages_total, last_position) OVER (PARTITION BY name ORDER BY last_position) AS messages,
			LAST(hardware_version, row) OVER (PARTITION BY name ORDER BY row) AS hardware_version,
			LAST(is_duplicate, last_position) OVER (PARTITION BY name ORDER BY last_position) AS is_duplicate,
			ROW_NUMBER() OVER (PARTITION BY name) AS row
		FROM (
			SELECT 
				*,
				SUM(messages) OVER (PARTITION BY name) AS messages_total,
				CASE
					WHEN COUNT(*) FILTER (WHERE is_version_valid IS TRUE) OVER (PARTITION BY name) > 1 THEN TRUE
					ELSE FALSE
				END as is_duplicate,
				ROW_NUMBER() OVER (PARTITION BY name ORDER BY is_version_valid IS FALSE, last_position DESC) AS row
			FROM (
				SELECT
					*,
					CASE
						WHEN original_address IS NULL THEN NULL
						WHEN messages >= 3 AND (original_address IS NULL OR fe.version IS NOT NULL) THEN TRUE
						ELSE FALSE
					END AS is_version_valid
				FROM senders
				LEFT JOIN flarm_expiry AS fe ON software_version = fe.version
			) AS sq
		) AS sq2
		WHERE sq2.row <= 2 AND (is_version_valid IS NULL OR is_version_valid IS TRUE)
	) AS sq3
	WHERE sq3.row = 1
) AS s
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
FROM opensky AS o

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
	time_bucket('1 day', p1d.ts) AS ts,
	p1d.src_call,
	
	AVG(p1d.normalized_quality - sq.normalized_quality) AS relative_quality,
	SUM(CASE WHEN p1d.normalized_quality < sq.percentile_10 THEN 1 ELSE 0 END) AS reporting_below_10,
	COUNT(*) AS total
FROM positions_1d AS p1d
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
	FROM positions_1d
	WHERE normalized_quality IS NOT NULL AND points_motion > 10
	GROUP BY 1, 2
) AS sq ON sq.ts = p1d.ts AND sq.receiver = p1d.receiver
GROUP BY 1, 2;

-- Create receiver view with ALL relevant informations
CREATE MATERIALIZED VIEW receivers_joined
AS
SELECT
  r.*,
  ST_X(r.location) AS lng,
  ST_Y(r.location) AS lat,
  CASE NOW() - r.last_position < INTERVAL'10 minutes'
    WHEN TRUE THEN 'ONLINE'
    ELSE 'OFFLINE'
  END AS online,
  CASE
    WHEN rs.points_fake > 10 THEN 'ERROR'
    WHEN rs.points_fake > 1 THEN 'WARNING'
    ELSE 'GOOD'
  END AS fake,
  CASE
    WHEN rs.ts IS NULL THEN 'BLIND'
    WHEN NOW() - rs.ts < INTERVAL'3 day' THEN 'GOOD'
    WHEN NOW() - rs.ts BETWEEN INTERVAL'3 day' AND INTERVAL'7 day' THEN 'WARNING'
    ELSE 'BLIND'
  END AS sighted,
  rs.distance AS "range",
  CASE
    WHEN rs.distance IS NULL THEN ''
    WHEN rs.distance < 10000 THEN 'BLIND'
    WHEN rs.distance < 25000 THEN 'WARNING'
    ELSE 'GOOD'
  END AS "range:check",
  rs.normalized_quality AS "quality",
  CASE
    WHEN rs.normalized_quality IS NULL THEN ''
    WHEN rs.normalized_quality < 10 THEN 'BLIND'
    WHEN rs.normalized_quality < 15 THEN 'WARNING'
    ELSE 'GOOD'
  END AS "quality:check"
FROM receivers AS r
LEFT JOIN
(
  SELECT
    p1d.receiver,
    MAX(p1d.ts) AS ts,
    SUM(p1d.points_fake) AS points_fake,
    MAX(p1d.normalized_quality) FILTER (WHERE p1d.plausibility = 0) AS normalized_quality,
    MAX(p1d.distance) FILTER (WHERE p1d.plausibility = 0) AS distance
  FROM positions_1d AS p1d
  WHERE
    NOW() - p1d.ts < INTERVAL'7 days'
    AND p1d.distance IS NOT NULL
  GROUP BY 1
) AS rs ON rs.receiver = r.name
ORDER BY r.iso2, r.name;


-- create ranking view with the ranking for today
CREATE MATERIALIZED VIEW ranking
AS
WITH daily_ranking AS (
	WITH daily_values AS (
		SELECT
			time_bucket('1 day', ts) AS ts,
			receiver,
			MAX(distance) AS distance,
			COUNT(*) AS sender_count,
			SUM(points_total) AS messages
		FROM positions_1h
		WHERE
			ts > NOW() - INTERVAL'30 days'
			AND dst_call IN ('APRS', 'OGFLR')
			AND plausibility = 0
		GROUP BY 1, 2
		HAVING SUM(points_fake) = 0
	)

	SELECT
		sq4.*,
		row_number() OVER (PARTITION BY sq4.ts ORDER BY points DESC) AS ranking_global,
		row_number() OVER (PARTITION BY sq4.ts, sq4.iso2 ORDER BY points DESC) AS ranking_country
	FROM (
		SELECT
			sq3.*,
			2 * sq3.distance_max + sq3.distance_avg AS points
		FROM (
			SELECT
				sq2.ts,
				sq2.receiver,
				r.iso2,
				r.altitude,
				sq2.distance,
				MAX(sq2.distance) OVER (PARTITION BY sq2.receiver ORDER BY sq2.ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS distance_max,
				AVG(sq2.distance) OVER (PARTITION BY sq2.receiver ORDER BY sq2.ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS distance_avg
			FROM (
				SELECT
					days_and_receivers.ts,
					days_and_receivers.receiver,
					COALESCE(d.distance, 0) AS distance,
					COALESCE(d.sender_count, 0) AS sender_count,
					COALESCE(d.messages, 0) AS messages
				FROM
				(
					SELECT *
					FROM (
						SELECT DISTINCT ts FROM daily_values
					) AS sq1,
					(
						SELECT DISTINCT receiver FROM daily_values
					) AS sq2
				) AS days_and_receivers
				LEFT JOIN daily_values AS d ON d.ts = days_and_receivers.ts AND d.receiver = days_and_receivers.receiver
			) AS sq2
			INNER JOIN receivers AS r ON sq2.receiver = r.name
		) AS sq3
	) AS sq4
)

SELECT
	dr.receiver,
	dr.iso2,
	iso2_to_emoji(dr.iso2) AS flag,
	dr.altitude,
	dr.ranking_global AS global,
	dr2.ranking_global - dr.ranking_global AS "global:delta",
	dr.ranking_country AS country,
	dr2.ranking_country - dr.ranking_country AS "country:delta",
	dr.distance,
	dr.distance_max AS "distance:max",
	dr.distance_avg AS "distance:avg",
	dr.points
FROM daily_ranking AS dr
LEFT JOIN daily_ranking AS dr2 ON dr.receiver = dr2.receiver
WHERE dr.ts = NOW()::DATE AND dr2.ts = dr.ts - INTERVAL'1 day'
