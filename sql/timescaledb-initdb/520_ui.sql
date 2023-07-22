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
	EXISTS (SELECT * FROM duplicates WHERE address = s.address) AS sender_is_duplicate,
	dj.*,
	fh.manufacturer AS flarm_hardware_manufacturer,
	fh.model AS flarm_hardware_model,
	fe.expiry_date AS flarm_expiry_date,
	a.name AS airport_name,
	a.code AS airport_code,
	a.iso2 AS airport_iso2,
	iso2_to_emoji(a.iso2) AS airport_flag,
	a.location AS airport_location,
	a.altitude AS airport_altitude,
	a.style AS airport_style,
	CASE
		WHEN s.location IS NOT NULL AND a.location IS NOT NULL THEN ST_DistanceSphere(s.location, a.location)
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
	iso2_to_emoji(i.iso2) AS icao24bit_flag,
	i.lower_limit AS icao24bit_lower_limit,
	i.upper_limit AS icao24bit_upper_limit,
	CASE
		WHEN dj.registration_iso2 IS NOT NULL THEN dj.registration_iso2
		WHEN i.iso2 IS NOT NULL THEN i.iso2
		ELSE ''
	END AS iso2,
	CASE
		WHEN dj.registration_iso2 IS NOT NULL THEN iso2_to_emoji(dj.registration_iso2)
		WHEN i.iso2 IS NOT NULL THEN iso2_to_emoji(i.iso2)
		ELSE ''
	END AS flag,
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
		WHEN EXISTS (SELECT * FROM duplicates WHERE address = s.address) THEN 'ERROR'
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
	END AS check_flarmnet_registration,
	CASE
		WHEN s.is_stealth THEN 'FLARM:STEALTH'
		WHEN s.is_notrack THEN 'FLARM:NOTRACK'
		WHEN dj.ddb_is_noident IS NULL THEN 'DDB:UNKNOWN'
		WHEN dj.ddb_is_noident IS TRUE THEN 'DDB:NOIDENT'
		WHEN dj.ddb_is_notrack IS TRUE THEN 'DDB:NOTRACK'
		ELSE 'OK'
	END AS privacy
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
CREATE UNIQUE INDEX senders_joined_idx ON senders_joined(sender_src_call);
CREATE INDEX senders_joined_airport_iso2_airport_name_idx ON senders_joined (airport_iso2, airport_name);
CREATE INDEX senders_joined_ddb_registration_idx ON senders_joined (ddb_registration);

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
-- cost: 1min
CREATE MATERIALIZED VIEW receivers_joined
AS
SELECT
	r.*,
	ST_X(r.location) AS lng,
	ST_Y(r.location) AS lat,
	c.iso_a2_eh,
	iso2_to_emoji(c.iso_a2_eh) AS flag,
	a.name AS airport_name,
	a.code AS airport_code,
	a.iso2 AS airport_iso2,
	a.location AS airport_location,
	a.altitude AS airport_altitude,
	a.style AS airport_style,
	CASE
		WHEN r.location IS NOT NULL AND a.location IS NOT NULL AND ST_DistanceSphere(r.location, a.location) < 2500
		THEN
			ST_DistanceSphere(r.location, a.location)
		ELSE NULL
	END as airport_distance,
	degrees(ST_Azimuth(r.location, a.location)) AS airport_radial,
	CASE NOW() - r.ts_last < INTERVAL'1 hour'
		WHEN TRUE THEN 'ONLINE'
		ELSE 'OFFLINE'
	END AS online,
	CASE
		WHEN rs.ts IS NULL THEN 'BLIND'
		WHEN rs.ts > NOW() - INTERVAL'3 day' THEN 'GOOD'
		WHEN rs.ts > NOW() - INTERVAL'7 day' THEN 'WARNING'
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
	rse.reboots AS "reboots",
	CASE
		WHEN rse.reboots IS NULL THEN ''
		WHEN rse.reboots < 14 THEN 'OK'
		WHEN rse.reboots < 28 THEN 'WARNING'
		ELSE 'ERROR'
	END AS "reboots:check",
	rse.server_changes AS "server_changes",
	CASE
		WHEN rse.server_changes IS NULL THEN ''
		WHEN rse.server_changes < 28 THEN 'OK'
		WHEN rse.server_changes < 56 THEN 'WARNING'
		ELSE 'ERROR'
	END AS "server_changes:check"
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
		AVG(rf_correction_automatic) AS rf_correction_automatic
	FROM statuses 
	WHERE
		ts > NOW() - INTERVAL '7 days'
	GROUP BY 1
) AS rst ON rs.receiver = rst.src_call
LEFT JOIN (
	SELECT
		src_call,
		SUM(CASE WHEN event & b'01'::INTEGER > 0 THEN 1 ELSE 0 END) AS reboots,
		SUM(CASE WHEN event & b'10'::INTEGER > 0 THEN 1 ELSE 0 END) AS server_changes
	FROM receiver_status_events
	WHERE ts > NOW() - INTERVAL '7 days'
	GROUP BY 1
) AS rse ON rs.receiver = rse.src_call
CROSS JOIN LATERAL (
	SELECT *
	FROM openaip
	ORDER BY openaip.location <-> r.location
	LIMIT 1
) AS a
LEFT JOIN countries AS c ON ST_Contains(c.geom, r.location)
ORDER BY c.iso_a2_eh, r.src_call;
CREATE UNIQUE INDEX receivers_joined_idx ON receivers_joined (src_call);

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
		HAVING MIN(distance_max) < 200000			-- ignore receivers who see nothing below 200km
	) AS sq ON r1d.ts = sq.ts AND r1d.receiver = sq.receiver AND r1d.distance_max = sq.distance_max
	LEFT JOIN receiver_position_states_1d AS rps1d ON r1d.ts = rps1d.ts AND r1d.receiver = rps1d.src_call
	WHERE
		rps1d.changed IS NULL OR rps1d.changed = 0	-- ignore receiver who are changing
		AND r1d.ts > NOW() - INTERVAL '30 days'
		AND rps1d.ts > NOW() - INTERVAL '30 days'
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
			r.flag,
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
		INNER JOIN receivers_joined AS r ON sq2.receiver = r.src_call
	) AS sq3
) AS sq4;
CREATE UNIQUE INDEX ranking_idx ON ranking (ts, ranking_global);
CREATE INDEX ranking_ts_idx ON ranking(ts);
