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
	s.original_addresses AS sender_original_addresses,
	dj.*,
	fh.devtype AS flarm_hardware_devtype,
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
	toa.airport_name AS takeoff_airport_name,
	toa.airport_iso2 AS takeoff_airport_iso2,
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
	r.distance_min AS records_distance_min,
	r.distance_max AS records_distance_max,
	r.normalized_quality_min AS records_normalized_quality_min,
	r.normalized_quality_max AS records_normalized_quality_max,
	iso2_to_emoji(dj.icao24bit_iso2) AS icao24bit_flag,
	CASE
		WHEN dj.registration_iso2 IS NOT NULL THEN dj.registration_iso2
		WHEN dj.icao24bit_iso2 IS NOT NULL THEN dj.icao24bit_iso2
		ELSE ''
	END AS iso2,
	CASE
		WHEN dj.registration_iso2 IS NOT NULL THEN iso2_to_emoji(dj.registration_iso2)
		WHEN dj.icao24bit_iso2 IS NOT NULL THEN iso2_to_emoji(dj.icao24bit_iso2)
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
		WHEN COALESCE(dj.ddb_cn, '') != '' THEN dj.ddb_cn
		WHEN COALESCE(w.cn, '') != '' THEN w.cn
		WHEN COALESCE(fn.cn, '') != '' THEN fn.cn
		ELSE ''
	END AS cn,
	CASE
		WHEN COALESCE(dj.ddb_model, '') != '' THEN dj.ddb_model
		WHEN COALESCE(o.model, '') != '' THEN o.model
		WHEN COALESCE(w.model, '') != '' THEN w.model
		WHEN COALESCE(fn.model, '') != '' THEN fn.model
		ELSE ''
	END AS model,
	CASE
		WHEN s.aircraft_type IS NULL OR dj.ddb_aircraft_types IS NULL THEN ''
		WHEN s.aircraft_type = ANY(dj.ddb_aircraft_types) THEN 'OK'
		WHEN dj.ddb_aircraft_types = ARRAY[0] THEN 'GENERIC'
		ELSE 'ERROR'
	END AS check_sender_ddb_aircraft_type,
	CASE 
		WHEN s.aircraft_type IS NULL OR dj.registration_aircraft_types IS NULL THEN ''
		WHEN s.aircraft_type = ANY(dj.registration_aircraft_types) THEN 'OK'
		WHEN dj.registration_aircraft_types::integer[] = ARRAY[0] THEN 'GENERIC'
		ELSE 'ERROR'
	END AS check_sender_registration_aircraft_type,
	CASE
		WHEN s.address_type IS NULL THEN ''
		WHEN dj.ddb_address_type IS NULL THEN 'UNKNOWN'
		WHEN s.address_type != dj.ddb_address_type THEN 'ERROR'
		ELSE 'OK'
	END AS check_sender_ddb_address_type,
	CASE
		WHEN EXISTS (SELECT * FROM duplicates WHERE address = s.address) THEN 'ERROR'
		ELSE 'OK'
	END AS check_sender_duplicate,
	CASE
		WHEN s.software_version IS NULL THEN ''
		WHEN s.software_version IS NOT NULL AND fe.expiry_date IS NULL THEN 'ERROR'
		ELSE 'OK'
	END AS check_sender_software_version_plausible, 
	CASE
		WHEN fe.expiry_date IS NULL THEN ''
		WHEN fe.expiry_date - NOW() > INTERVAL'90 days' THEN 'OK'
		WHEN fe.expiry_date - NOW() > INTERVAL'1 day' THEN 'WARNING'
		ELSE 'ERROR'
	END AS check_sender_expiry_date,
	CASE
		WHEN array_length(dj.registration_addresses, 1) = 1 THEN 'OK'
		ELSE 'ERROR'
	END AS check_ddb_registration,
	CASE
		WHEN o.registration IS NULL OR o.registration = '' THEN ''
		WHEN dj.ddb_registration IS NULL OR dj.ddb_registration = '' THEN 'WARNING'
		WHEN dj.ddb_registration IS NOT NULL AND o.registration IS NOT NULL AND dj.ddb_registration = o.registration THEN 'OK'
		ELSE 'ERROR'
	END AS check_ddb_opensky_registration,
	CASE
		WHEN w.registration IS NULL OR w.registration = '' THEN ''
		WHEN dj.ddb_registration IS NULL OR dj.ddb_registration = '' THEN 'WARNING'
		WHEN dj.ddb_registration IS NOT NULL AND w.registration IS NOT NULL AND dj.ddb_registration = w.registration THEN 'OK'
		ELSE 'ERROR'
	END AS check_ddb_weglide_registration,
	CASE
		WHEN fn.registration IS NULL OR fn.registration = '' THEN ''
		WHEN dj.ddb_registration IS NULL OR dj.ddb_registration = '' THEN 'WARNING'
		WHEN dj.ddb_registration IS NOT NULL AND fn.registration IS NOT NULL AND dj.ddb_registration = fn.registration THEN 'OK'
		ELSE 'ERROR'
	END AS check_ddb_flarmnet_registration,
	CASE
		WHEN s.is_stealth THEN 'FLARM:STEALTH'
		WHEN s.is_notrack THEN 'FLARM:NOTRACK'
		WHEN dj.ddb_is_noident IS NULL THEN 'DDB:UNKNOWN'
		WHEN dj.ddb_is_noident IS TRUE THEN 'DDB:NOIDENT'
		WHEN dj.ddb_is_notrack IS TRUE THEN 'DDB:NOTRACK'
		WHEN dj.ddb_registration ~ '^[DF]\-[Xx].{3}$' THEN 'REG:NOIDENT'
		WHEN dj.ddb_registration LIKE 'X-%' THEN 'REG:NOIDENT'
		WHEN dj.ddb_registration IS NULL AND dj.ddb_model_type in (1,2,3,4) THEN 'REG:NOIDENT'
		ELSE 'OK'
	END AS privacy
FROM senders AS s
LEFT JOIN ddb_joined AS dj ON s.address = dj.ddb_address
LEFT JOIN flarm_hardware AS fh ON s.hardware_version = fh.hwver
LEFT JOIN flarm_expiry AS fe ON s.software_version = fe.version
LEFT JOIN opensky AS o ON s.address = o.address
LEFT JOIN weglide AS w ON s.address = w.address
LEFT JOIN flarmnet AS fn ON s.address = fn.address
LEFT JOIN (
	SELECT 
		src_call,
		LAST(airport_name, receiver_ts) AS airport_name,
		LAST(airport_iso2, receiver_ts) AS airport_iso2
	FROM takeoffs
	WHERE event IN (0, 2, 4, 6)
	GROUP BY src_call
) AS toa ON s.src_call = toa.src_call
LEFT JOIN (
	SELECT
		src_call,
	
		MIN(distance_min) AS distance_min,
		MAX(distance_max) AS distance_max,
		MIN(normalized_quality_min) AS normalized_quality_min,
		MAX(normalized_quality_max) AS normalized_quality_max,
	
		COUNT(*) AS records
	FROM records_1d
	WHERE ts BETWEEN NOW()::DATE - INTERVAL'30 days' AND NOW()::DATE
		 AND (src_call LIKE 'ICA%' OR src_call LIKE 'FLR%')
	GROUP BY 1
) AS r ON s.src_call = r.src_call
CROSS JOIN LATERAL (
	SELECT *
	FROM openaip
	ORDER BY openaip.location <-> s.location
	LIMIT 1
) AS a;
CREATE UNIQUE INDEX senders_joined_idx ON senders_joined(sender_src_call);
CREATE INDEX senders_joined_airport_iso2_airport_name_idx ON senders_joined (airport_iso2, airport_name);
CREATE INDEX senders_joined_ddb_registration_idx ON senders_joined (ddb_registration);

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
	CASE WHEN prs.src_call IS NOT NULL THEN prs.antenna ELSE s.antenna END AS setup_antenna,
	CASE WHEN prs.src_call IS NOT NULL THEN prs.filter ELSE s.filter END AS setup_filter,
	CASE WHEN prs.src_call IS NOT NULL THEN prs.amplifier ELSE s.amplifier END AS setup_amplifier,
	CASE WHEN prs.src_call IS NOT NULL THEN prs.dongle ELSE s.dongle END AS setup_dongle,
	prs.club AS setup_club,
	prs.email AS setup_email,
	prs.website AS setup_website,
	prs.note AS setup_note,
	CASE
		WHEN r.location IS NOT NULL AND a.location IS NOT NULL AND ST_DistanceSphere(r.location, a.location) < 2500
		THEN
			ST_DistanceSphere(r.location, a.location)
		ELSE NULL
	END as airport_distance,
	degrees(ST_Azimuth(r.location, a.location)) AS airport_radial,
	CASE NOW() - r.ts_last_position < INTERVAL'1 hour'
		WHEN TRUE THEN 'ONLINE'
		ELSE 'OFFLINE'
	END AS online,
	CASE
		WHEN r.ts_last_sender IS NULL THEN 'BLIND'
		WHEN r.ts_last_sender > NOW() - INTERVAL'3 day' THEN 'GOOD'
		WHEN r.ts_last_sender > NOW() - INTERVAL'7 day' THEN 'WARNING'
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
	ers.reboots AS "reboots",
	CASE
		WHEN ers.reboots IS NULL THEN ''
		WHEN ers.reboots < 14 THEN 'OK'
		WHEN ers.reboots < 28 THEN 'WARNING'
		ELSE 'ERROR'
	END AS "reboots:check",
	ers.server_changes AS "server_changes",
	CASE
		WHEN ers.server_changes IS NULL THEN ''
		WHEN ers.server_changes < 28 THEN 'OK'
		WHEN ers.server_changes < 56 THEN 'WARNING'
		ELSE 'ERROR'
	END AS "server_changes:check",
	ers.version_changes AS "version_changes",
	ers.platform_changes AS "platform_changes",
	erp.altitude_changes AS "altitude_changes",
	CASE
		WHEN erp.altitude_changes IS NULL THEN ''
		WHEN erp.altitude_changes = 0 THEN 'OK'
		ELSE 'WARNING'
	END AS "altitude_changes:check",
	erp.location_changes AS "location_changes",
	CASE
		WHEN erp.location_changes IS NULL THEN ''
		WHEN erp.location_changes = 0 THEN 'OK'
		ELSE 'ERROR'
	END AS "location_changes:check"
FROM receivers AS r
LEFT JOIN (
	SELECT
		receiver,
		MAX(distance_confirmed) AS distance_max
	FROM statistics_receiver_15m
	WHERE
		ts > NOW() - INTERVAL'7 days'
		AND distance_confirmed IS NOT NULL
	GROUP BY 1
) AS rs ON rs.receiver = r.src_call
LEFT JOIN (
	SELECT
		src_call,
		MAX(cpu_temperature) AS cpu_temperature,
		AVG(rf_correction_automatic) AS rf_correction_automatic
	FROM statuses
	WHERE
		ts > NOW() - INTERVAL'7 days'
	GROUP BY 1
) AS rst ON rst.src_call = r.src_call
LEFT JOIN (
	SELECT
		src_call,
		COUNT(*) FILTER (WHERE event & b'0001'::INTEGER != 0) AS reboots,
		COUNT(*) FILTER (WHERE event & b'0010'::INTEGER != 0) AS server_changes,
		COUNT(*) FILTER (WHERE event & b'0100'::INTEGER != 0) AS version_changes,
		COUNT(*) FILTER (WHERE event & b'1000'::INTEGER != 0) AS platform_changes
	FROM events_receiver_status
	WHERE ts > NOW() - INTERVAL '7 days'
	GROUP BY 1
) AS ers ON ers.src_call = r.src_call
LEFT JOIN (
	SELECT
		src_call,
		COUNT(*) FILTER (WHERE event & b'01'::INTEGER != 0) AS altitude_changes,
		COUNT(*) FILTER (WHERE event & b'10'::INTEGER != 0) AS location_changes
	FROM events_receiver_position
	WHERE ts > NOW() - INTERVAL'7 days'
	GROUP BY src_call
) AS erp ON erp.src_call = r.src_call
CROSS JOIN LATERAL (
	SELECT *
	FROM openaip
	ORDER BY openaip.location <-> r.location
	LIMIT 1
) AS a
LEFT JOIN receiver_setups AS s ON s.receiver = r.src_call
LEFT JOIN countries AS c ON ST_Contains(c.geom, r.location)
LEFT JOIN (
	SELECT
		src_call,
	
		LAST(antenna, ts) AS antenna,
		LAST(filter, ts) AS filter,
		LAST(amplifier, ts) AS amplifier,
		LAST(dongle, ts) AS dongle,
		LAST(club, ts) AS club,
		LAST(email, ts) AS email,
		LAST(website, ts) AS website,
		LAST(note, ts) AS note
	FROM positions_receiver_setup_1h
	GROUP BY 1
) AS prs ON prs.src_call = r.src_call
WHERE
	r.version IS NOT NULL
	AND r.platform IS NOT NULL
ORDER BY c.iso_a2_eh, r.src_call;
CREATE UNIQUE INDEX receivers_joined_idx ON receivers_joined (src_call);

-- create ranking view with the ranking for today
CREATE MATERIALIZED VIEW ranking
AS
WITH day_and_receivers AS (
	SELECT
	*
FROM
	(SELECT DISTINCT ts FROM rankings_1d) AS inner1,
	(SELECT DISTINCT receiver FROM rankings_1d) AS inner2
)

SELECT
	sq4.ts,
	sq4.receiver,
	sq4.iso_a2_eh,
	sq4.flag,
	sq4.altitude_max AS altitude,
	sq4.distance_max AS distance,
	sq4.ts_first,
	sq4.ts_last,
	sq4.src_call,
	sq4.distance_max_30 AS distance_max,
	sq4.distance_avg_30 AS distance_avg,
	sq4.online,
	sq4.points,
	sq4.ranking_global,
	sq4.ranking_country
FROM (
	SELECT
		sq3.*,
	
		row_number() OVER (PARTITION BY sq3.ts ORDER BY points DESC) AS ranking_global,
		row_number() OVER (PARTITION BY sq3.ts, sq3.iso_a2_eh ORDER BY points DESC) AS ranking_country
	FROM (
		SELECT
			sq2.*,
			
			sq2.distance_avg_30 AS points
		FROM (
			SELECT
				sq.*,
			
				r.iso_a2_eh,
				r.flag,
				
				MAX(distance_max) OVER (PARTITION BY sq.receiver ORDER BY sq.ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS distance_max_30,
				AVG(distance_max) OVER (PARTITION BY sq.receiver ORDER BY sq.ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS distance_avg_30,
				AVG(sq.online) OVER (PARTITION BY sq.receiver ORDER BY sq.ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS online_avg_30
			FROM (
				SELECT
					dar.ts,
					dar.receiver,
				
					r1d.src_call,
					r1d.ts_first,
					r1d.ts_last,
					COALESCE(r1d.distance_min, 0) AS distance_min,
					COALESCE(r1d.distance_max, 0) AS distance_max,
					COALESCE(r1d.altitude_min, 0) AS altitude_min,
					COALESCE(r1d.altitude_max, 0) AS altitude_max,
					COALESCE(r1d.normalized_quality_min, 0) AS normalized_quality_min,
					COALESCE(r1d.normalized_quality_max, 0) AS normalized_quality_max,
					COALESCE(r1d.online, 0) AS online
				FROM day_and_receivers AS dar
				LEFT JOIN (
					SELECT * FROM rankings_1d WHERE is_disqualified IS FALSE
				) AS r1d ON r1d.ts = dar.ts AND r1d.receiver = dar.receiver
			) AS sq
			INNER JOIN receivers_joined AS r ON r.src_call = sq.receiver
			WHERE r.iso_a2_eh IS NOT NULL
		) AS sq2
	) AS sq3
	WHERE points > 0
) AS sq4;
CREATE UNIQUE INDEX ranking_idx ON ranking (ts, ranking_global);
CREATE INDEX ranking_ts_idx ON ranking(ts);

-- create logbook
CREATE MATERIALIZED VIEW logbook
AS
SELECT
	*,
	to_char(COALESCE(sq.receiver_ts_takeoff, sq.receiver_ts_landing), 'YYYY-MM-DD') AS str_date,
	to_char(sq.receiver_ts_takeoff, 'HH24:MI') AS str_takeoff,
	to_char(sq.receiver_ts_landing, 'HH24:MI') AS str_landing,
	to_char(date_trunc('minutes', sq.receiver_ts_landing) - date_trunc('minutes', sq.receiver_ts_takeoff), 'HH24:MI') AS str_duration
FROM (
	SELECT
		src_call,
	
		CASE WHEN is_takeoff IS TRUE THEN receiver_ts ELSE NULL END AS receiver_ts_takeoff,
		CASE WHEN is_takeoff IS TRUE THEN airport_name ELSE NULL END AS airport_name_takeoff,
		CASE WHEN is_takeoff IS TRUE THEN airport_iso2 ELSE NULL END AS airport_iso2_takeoff,
		CASE WHEN is_takeoff_n IS FALSE THEN receiver_ts_n ELSE NULL END AS receiver_ts_landing,
		CASE WHEN is_takeoff_n IS FALSE THEN airport_name_n ELSE NULL END AS airport_name_landing,
		CASE WHEN is_takeoff_n IS FALSE THEN airport_iso2_n ELSE NULL END AS airport_iso2_landing,
	
		CASE
			WHEN is_takeoff IS TRUE AND is_takeoff_n IS NULL THEN 'WARNING: NOT LANDED YET'
			WHEN is_takeoff IS TRUE AND is_takeoff_n IS TRUE THEN 'ERROR: NO LANDING FOUND'
			WHEN is_takeoff IS FALSE AND is_takeoff_n IS FALSE THEN 'ERROR: NO TAKEOFF FOUND'
			WHEN is_takeoff IS TRUE AND is_takeoff_n IS FALSE THEN 'OK'
			ELSE 'ERROR: ' || COALESCE(is_takeoff_p::TEXT, 'NULL') || ' ' || COALESCE(is_takeoff::TEXT, 'NULL') || ' ' || COALESCE(is_takeoff_n::TEXT, 'NULL')
		END AS check_state
	FROM (
		SELECT
			t.src_call,
			
			receiver_ts,
			LEAD(receiver_ts) OVER w AS receiver_ts_n,
		
			airport_name,
			LEAD(airport_name) OVER w AS airport_name_n,
	
			airport_iso2,
			LEAD(airport_iso2) OVER w AS airport_iso2_n,
		
			LAG(event) OVER w IN (0, 2, 4, 6) AS is_takeoff_p,
			event IN (0, 2, 4, 6) AS is_takeoff,
			LEAD(event) OVER w IN (0, 2, 4, 6) AS is_takeoff_n
		FROM takeoffs AS t
		WINDOW w AS (PARTITION BY src_call ORDER BY receiver_ts)
	) AS sq
	WHERE
		is_takeoff IS TRUE OR is_takeoff_n IS FALSE
) AS sq;
