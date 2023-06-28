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
	fn.registration AS flarmnet_registration,
	fn.cn AS flarmnet_cn,
	fn.model AS flarmnet_model,
	fn.radio AS flarmnet_radio,
	q.relative_quality AS quality_relative_quality,
	1.0 / 10^(-q.relative_quality/20.0) AS quality_relative_range,
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
		WHEN COALESCE(o.registration, '') != '' THEN o.registration
		WHEN COALESCE(w.registration, '') != '' THEN w.registration
		WHEN COALESCE(fn.registration, '') != '' THEN fn.registration
		ELSE ''
	END AS registration,
	CASE
		WHEN COALESCE(dj.ddb_model, '') != '' THEN dj.ddb_model
		WHEN COALESCE(o.model, '') != '' THEN o.model
		WHEN COALESCE(w.model, '') != '' THEN w.model
		WHEN COALESCE(fn.model, '') != '' THEN fn.model
		ELSE ''
	END AS model,
	CASE 
		WHEN s.aircraft_type IS NULL THEN ''
		WHEN dj.registration_aircraft_types IS NULL THEN 'UNKNOWN'
		WHEN s.aircraft_type = ANY(dj.registration_aircraft_types) THEN 'OK'
		WHEN 0 = ALL(dj.registration_aircraft_types) THEN 'GENERIC'
		ELSE 'ERROR'
	END AS check_registration_aircraft_types,
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
		WHEN s.aircraft_type IS NULL THEN 'UNKNOWN'
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
		ELSE 'ERROR'
	END AS check_model_type,
	CASE
		WHEN dj.ddb_address_type IS NULL THEN ''
		WHEN s.address_type IS NULL THEN 'UNKNOWN'
		WHEN s.address_type != dj.ddb_address_type THEN 'ERROR'
		ELSE 'OK'
	END AS check_address_type,
	CASE
		WHEN s.is_duplicate THEN 'ERROR'
		ELSE 'OK'
	END AS check_sender_duplicate,
	CASE
		WHEN s.software_version IS NULL THEN ''
		WHEN s.software_version != NULL AND fe.expiry_date IS NULL THEN 'ERROR'
		ELSE 'OK'
	END AS check_sender_software_version_plausible, 
	CASE
		WHEN fe.expiry_date IS NULL THEN ''
		WHEN fe.expiry_date - NOW() > INTERVAL'6 months' THEN 'OK'
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
	END AS check_weglide_registration,
	CASE
		WHEN dj.ddb_registration IS NULL OR fn.registration IS NULL THEN ''
		WHEN dj.ddb_registration IS NOT NULL AND fn.registration IS NOT NULL AND dj.ddb_registration = fn.registration THEN 'OK'
		ELSE 'ERROR'
	END AS check_flarmnet_registration
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
LEFT JOIN flarmnet AS fn ON s.address = fn.address
LEFT JOIN (
	SELECT
		src_call,
		AVG(relative_quality) AS relative_quality
	FROM senders_relative_qualities
	WHERE ts > now() - INTERVAL '30 days'
	GROUP BY 1
) AS q ON s.name = q.src_call
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
FROM weglide AS w

UNION

SELECT
  'Flarmnet' AS "source",
  f.address AS "address",
  f.registration AS "registration",
  f.model AS "model"
FROM flarmnet AS f;

CREATE INDEX ON registration_joined(address, registration);
CREATE INDEX ON registration_joined(registration, address);

-- Create normalized sender qualities
CREATE MATERIALIZED VIEW senders_relative_qualities
AS
WITH qualities
AS (
	SELECT
		time_bucket('1 day', ts) AS ts,	
		receiver,
		src_call,

		MAX(normalized_quality_max) AS normalized_quality_max,
		SUM(points_total) AS points_total
	FROM sender_positions_1d
	WHERE
		normalized_quality_max IS NOT NULL
		AND plausibility IS NOT NULL
		AND plausibility != -1
		AND plausibility & b'11110000111111'::integer = 0 -- no jumps, no singles, no fakes
	GROUP BY 1, 2, 3
)

SELECT
	q.ts,
	q.src_call,
	
	AVG(q.normalized_quality_max - sq.normalized_quality) AS relative_quality,
	SUM(CASE WHEN q.normalized_quality_max < sq.percentile_10 THEN 1 ELSE 0 END) AS reporting_below_10,
	SUM(q.points_total) AS points_total,
	COUNT(*) AS total
FROM qualities AS q
INNER JOIN (
	SELECT
		ts,
		receiver,

		AVG(normalized_quality_max) AS normalized_quality,
		COUNT(*) AS senders_count,
		SUM(points_total) AS points_total,
		PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY normalized_quality_max) AS percentile_10
	FROM qualities
	GROUP BY 1, 2
) AS sq ON q.ts = sq.ts AND q.receiver = sq.receiver
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
  END AS "quality:check",
  rst.cpu_temperature AS "cpu_temp",
  CASE
    WHEN rst.cpu_temperature IS NULL THEN ''
    WHEN rst.cpu_temperature < 70 THEN 'OK'
	WHEN rst.cpu_temperature < 80 THEN 'WARNING'
	ELSE 'ERROR'
  END AS "cpu_temp:check",
  rst.rf_correction_automatic AS "rf_corr",
  CASE
    WHEN rst.rf_correction_automatic IS NULL THEN ''
	WHEN ABS(rst.rf_correction_automatic) < 10 THEN 'OK'
	WHEN ABS(rst.rf_correction_automatic) < 20 THEN 'WARNING'
	ELSE 'ERROR'
  END AS "rf_corr:check",
  rst.reboots AS "reboots",
  CASE
    WHEN rst.reboots IS NULL THEN ''
	WHEN rst.reboots < 14 THEN 'OK'
	WHEN rst.reboots < 28 THEN 'WARNING'
	ELSE 'ERROR'
  END AS "reboots:check"
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
    ts > NOW() - INTERVAL'7 days'
    AND p1d.distance IS NOT NULL
  GROUP BY 1
) AS rs ON rs.receiver = r.name
LEFT JOIN (
  SELECT
    src_call,
    MAX(cpu_temperature) AS cpu_temperature,
    AVG(rf_correction_automatic) AS rf_correction_automatic,
    SUM(CASE COALESCE(senders_messages, 0) < COALESCE(senders_messages_prev, 0) WHEN TRUE THEN 1 ELSE 0 END) AS reboots
  FROM (
    SELECT 
      *,
      LAG(senders_messages) OVER (PARTITION BY src_call ORDER BY ts) AS senders_messages_prev
    FROM statuses 
    WHERE
      ts > NOW() - INTERVAL '7 days'
      AND dst_call IN ('APRS', 'OGNSDR')
      AND src_call NOT LIKE 'PW%'
  ) AS sq
  GROUP BY 1
) AS rst ON rs.receiver = rst.src_call
ORDER BY r.iso2, r.name;


-- create ranking view with the ranking for today
CREATE MATERIALIZED VIEW ranking
AS
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
			sq2.distance_ts,
			sq2.distance_src_call,
			MAX(COALESCE(sq2.distance, 0)) OVER (PARTITION BY sq2.receiver ORDER BY sq2.ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS distance_max,
			AVG(COALESCE(sq2.distance, 0)) OVER (PARTITION BY sq2.receiver ORDER BY sq2.ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS distance_avg
		FROM (
			SELECT
				days_and_receivers.ts,
				days_and_receivers.receiver,
				r1d.distance,
				r1d.distance_ts,
				r1d.distance_src_call
			FROM
			(
				SELECT *
				FROM (
					SELECT DISTINCT ts FROM records_1d WHERE ts > NOW() - INTERVAL '30 days'
				) AS sq1,
				(
					SELECT DISTINCT receiver FROM records_1d WHERE ts > NOW() - INTERVAL '30 days'
				) AS sq2
			) AS days_and_receivers
			LEFT JOIN records_1d AS r1d ON r1d.ts = days_and_receivers.ts AND r1d.receiver = days_and_receivers.receiver
			LEFT JOIN receiver_positions_1d AS rp1d ON rp1d.ts = days_and_receivers.ts AND rp1d.src_call = days_and_receivers.receiver
			WHERE
				COALESCE(rp1d.location_is_stable, TRUE) IS TRUE
				AND COALESCE(rp1d.altitude_is_stable, TRUE) IS TRUE
		) AS sq2
		INNER JOIN receivers AS r ON sq2.receiver = r.name
	) AS sq3
) AS sq4;
