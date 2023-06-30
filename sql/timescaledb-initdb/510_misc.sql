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
CREATE INDEX idx_ddb_joined_address ON ddb_joined (ddb_address);

-- create sender view with ALL relevant informations
-- cost: 5s
CREATE MATERIALIZED VIEW senders_joined
AS
SELECT
	s.src_call AS sender_src_call,
	s.ts_first AS sender_ts_first,
	s.ts_last AS sender_ts_last,
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
			(s.src_call LIKE 'ICA%' OR s.src_call LIKE 'PAW%')
			AND dj.registration_iso2 IS NOT NULL
			AND i.iso2 IS NOT NULL
			AND dj.registration_iso2 != i.iso2
		THEN 'ERROR'
		WHEN 
			(s.src_call LIKE 'ICA%' OR s.src_call LIKE 'PAW%')
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
FROM senders AS s
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
	FROM sender_relative_qualities
	WHERE ts > now() - INTERVAL '30 days'
	GROUP BY 1
) AS q ON s.src_call = q.src_call
LEFT JOIN icao24bit AS i ON s.address BETWEEN lower_limit AND upper_limit
CROSS JOIN LATERAL (
	SELECT *
	FROM openaip
	ORDER BY openaip.location <-> s.location
	LIMIT 1
) AS a;
CREATE INDEX idx_senders_joined_airport_iso2_airport_name ON senders_joined (airport_iso2, airport_name);

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

CREATE INDEX ON registration_joined(address, registration);
CREATE INDEX ON registration_joined(registration, address);

-- Create receiver view with ALL relevant informations
-- cost: 7s
CREATE MATERIALIZED VIEW receivers_joined
AS
SELECT
  r.*,
  ST_X(r.location) AS lng,
  ST_Y(r.location) AS lat,
  CASE NOW() - r.ts_last < INTERVAL'10 minutes'
    WHEN TRUE THEN 'ONLINE'
    ELSE 'OFFLINE'
  END AS online,
  CASE
    WHEN rs.ts IS NULL THEN 'BLIND'
    WHEN NOW() - rs.ts < INTERVAL'3 day' THEN 'GOOD'
    WHEN NOW() - rs.ts BETWEEN INTERVAL'3 day' AND INTERVAL'7 day' THEN 'WARNING'
    ELSE 'BLIND'
  END AS sighted,
  rs.distance_max AS "range",
  CASE
    WHEN rs.distance_max IS NULL THEN ''
    WHEN rs.distance_max < 10000 THEN 'BLIND'
    WHEN rs.distance_max < 25000 THEN 'WARNING'
    ELSE 'GOOD'
  END AS "range:check",
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
    MAX(p1d.distance_max) AS distance_max
  FROM receiver_statistics_1d AS p1d
  WHERE
    ts > NOW() - INTERVAL'7 days'
    AND p1d.distance_max IS NOT NULL
  GROUP BY 1
) AS rs ON rs.receiver = r.src_call
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
ORDER BY r.iso_a2_eh, r.src_call;


-- create ranking view with the ranking for today
CREATE MATERIALIZED VIEW ranking
AS
WITH records AS (
	SELECT
		r1d.*
	FROM records_1d AS r1d
	INNER JOIN (
		SELECT
			ts,
			receiver,

			MAX(distance_max) AS distance_max
		FROM records_1d
		WHERE ts > NOW() - INTERVAL '30 days'			-- consider the last 30 days
		GROUP BY 1, 2
		HAVING MIN(distance_max) < 200000				-- ignore receivers who see nothing below 200km
	) AS sq ON r1d.ts = sq.ts AND r1d.receiver = sq.receiver AND r1d.distance_max = sq.distance_max
	LEFT JOIN receiver_position_states_1d AS rps1d ON r1d.ts = rps1d.ts AND r1d.receiver = rps1d.src_call
	WHERE rps1d.changed IS NULL OR rps1d.changed = 0	-- ignore receiver who are changing
	ORDER BY ts, receiver
)

SELECT
	sq4.*,
	row_number() OVER (PARTITION BY sq4.ts ORDER BY points DESC) AS ranking_global,
	row_number() OVER (PARTITION BY sq4.ts, sq4.iso_a2_eh ORDER BY points DESC) AS ranking_country
FROM (
	SELECT
		sq3.*,
		2 * sq3.distance_max + sq3.distance_avg AS points
	FROM (
		SELECT
			sq2.ts,
			sq2.receiver,
			r.iso_a2_eh,
			r.altitude,
			sq2.distance_max AS distance,
			sq2.ts_first,
			sq2.ts_last,
			sq2.src_call,
			MAX(COALESCE(sq2.distance_max, 0)) OVER (PARTITION BY sq2.receiver ORDER BY sq2.ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS distance_max,
			AVG(COALESCE(sq2.distance_max, 0)) OVER (PARTITION BY sq2.receiver ORDER BY sq2.ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS distance_avg
		FROM (
			SELECT
				days_and_receivers.ts,
				days_and_receivers.receiver,

				r1d.distance_max,
				r1d.ts_first,
				r1d.ts_last,
				r1d.src_call
			FROM
			(
				SELECT
					*
				FROM
					(SELECT DISTINCT ts FROM records) AS inner1,
					(SELECT DISTINCT receiver FROM records) AS inner2
			) AS days_and_receivers
			LEFT JOIN records AS r1d ON r1d.ts = days_and_receivers.ts AND r1d.receiver = days_and_receivers.receiver
		) AS sq2
		INNER JOIN receivers AS r ON sq2.receiver = r.src_call
	) AS sq3
) AS sq4;
