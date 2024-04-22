CREATE OR REPLACE FUNCTION update_events_receiver_status(my_interval INTERVAL)
RETURNS INTEGER AS $$
DECLARE
    processed_event_rows INTEGER;
BEGIN

EXECUTE '
	WITH range_to_compute AS (
		SELECT
			MAX(ts) AS ts_start,
			MAX(ts) + ''' || my_interval || ''' AS ts_end
		FROM (
			SELECT
				FIRST(ts, ts) AS ts
			FROM statuses
		
			UNION
		
			SELECT
				LAST(ts, ts) AS ts
			FROM events_receiver_status
		)
	)
	
	INSERT INTO events_receiver_status(ts, src_call, receiver, version, platform, event, senders_messages)
	SELECT
		MIN(ts) AS ts,
		src_call,
	
		receiver,
		version,
		platform,
		MAX(event) AS event,
		MAX(senders_messages) AS senders_messages
	FROM (
		SELECT
			*,
		
			-- event
			-- bit 0: reboot
			-- bit 1: server change
			-- bit 2: version change
			-- bit 3: platform change
			0
			+ CASE WHEN reboot THEN 1 ELSE 0 END
			+ CASE WHEN server_change THEN 2 ELSE 0 END
			+ CASE WHEN version_change THEN 4 ELSE 0 END
			+ CASE WHEN platform_change THEN 8 ELSE 0 END
			AS event,
		
			SUM(CASE WHEN reboot OR server_change OR version_change OR platform_change THEN 1 ELSE 0 END) OVER (PARTITION BY src_call, receiver, version, platform ORDER BY ts) AS inner_group
		FROM (
			SELECT
				*,
			
				COALESCE(senders_messages_prev, 0) > senders_messages AS reboot,
				COALESCE(receiver_prev, receiver) != receiver AS server_change,
				COALESCE(version_prev, version) != version AS version_change,
				COALESCE(platform_prev, platform) != platform AS platform_change
			FROM (
				SELECT
					*,
			
					LAG(receiver) OVER (PARTITION BY src_call ORDER BY ts) AS receiver_prev,
					LAG(version) OVER (PARTITION BY src_call ORDER BY ts) AS version_prev,
					LAG(platform) OVER (PARTITION BY src_call ORDER BY ts) AS platform_prev,
					LAG(senders_messages) OVER (PARTITION BY src_call ORDER BY ts) AS senders_messages_prev
				FROM (
					SELECT
						ts,
						src_call,
			
						receiver,
						version,
						platform,
						COALESCE(senders_messages, 0) AS senders_messages
					FROM statuses				
					WHERE
						ts BETWEEN (SELECT ts_start FROM range_to_compute) AND (SELECT ts_end FROM range_to_compute)
						AND (dst_call = ''OGNSDR'' OR (dst_call = ''APRS'' AND receiver LIKE ''GLIDERN%''))
						AND version IS NOT NULL
						AND platform IS NOT NULL

					UNION
				
					SELECT
						ts,
						src_call,
			
						receiver,
						version,
						platform,
						senders_messages
					FROM (
						SELECT
							*,
							ROW_NUMBER() OVER (PARTITION BY src_call ORDER BY ts DESC) AS rn
						FROM events_receiver_status
					) AS sq
					WHERE sq.rn = 1
				) AS sq
			) AS sq2
		) AS sq3
	) AS sq4
	GROUP BY 2, 3, 4, 5, inner_group
	ORDER BY 1
	ON CONFLICT (ts, src_call) DO UPDATE
		SET
			senders_messages = excluded.senders_messages;
	';

GET DIAGNOSTICS processed_event_rows = ROW_COUNT;
RETURN processed_event_rows;

END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION update_events_receiver_position(my_interval INTERVAL)
RETURNS INTEGER AS $$
DECLARE
    processed_event_rows INTEGER;
BEGIN

EXECUTE '
	WITH range_to_compute AS (
		SELECT
			MAX(ts) AS ts_start,
			MAX(ts) + ''' || my_interval || ''' AS ts_end
		FROM (
			SELECT
				FIRST(ts, ts) AS ts
			FROM positions
		
			UNION
		
			SELECT
				LAST(ts, ts) AS ts
			FROM events_receiver_position
		)
	)
	
	INSERT INTO events_receiver_position(ts, src_call, altitude, location, event)
	SELECT
		ts,
		src_call,
	
		altitude,
		location,
	
		-- event
		-- bit 0: altitude change
		-- bit 1: location change
		0
		+ CASE WHEN altitude_change THEN 1 ELSE 0 END
		+ CASE WHEN location_change THEN 2 ELSE 0 END
		AS event
	FROM (
		SELECT
			*,
		
			altitude_prev IS NOT NULL AND altitude_prev != altitude AS altitude_change,
			location_prev IS NOT NULL AND (ST_X(location_prev) != ST_X(location) OR ST_Y(location_prev) != ST_Y(location)) AS location_change
		FROM (
			SELECT
				*,
		
				LAG(altitude) OVER (PARTITION BY src_call ORDER BY ts) AS altitude_prev,
				LAG(location) OVER (PARTITION BY src_call ORDER BY ts) AS location_prev
			FROM (
				SELECT
					ts,
					src_call,
		
					altitude,
					location
				FROM positions
				WHERE
					ts BETWEEN (SELECT ts_start FROM range_to_compute) AND (SELECT ts_end FROM range_to_compute)
					AND (dst_call = ''OGNSDR'' OR (dst_call = ''APRS'' AND receiver LIKE ''GLIDERN%''))
					AND altitude IS NOT NULL
					AND location IS NOT NULL
	
				UNION
			
				SELECT
					ts,
					src_call,
		
					altitude,
					location
				FROM (
					SELECT
						*,
						ROW_NUMBER() OVER (PARTITION BY src_call ORDER BY ts DESC) AS rn
					FROM events_receiver_position
				) AS sq
				WHERE sq.rn = 1
			) AS sq
		) AS sq2
	) AS sq3
	WHERE
		(altitude_prev IS NULL OR altitude_change IS TRUE)
		OR (location_prev IS NULL OR location_change IS TRUE)
	ORDER BY 1
	ON CONFLICT DO NOTHING;
	';

GET DIAGNOSTICS processed_event_rows = ROW_COUNT;
RETURN processed_event_rows;

END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION update_events_sender_position(my_interval INTERVAL)
RETURNS INTEGER AS $$
DECLARE
    processed_event_rows INTEGER;
BEGIN

EXECUTE '
	WITH range_to_compute AS (
		SELECT
			MAX(ts) AS ts_start,
			MAX(ts) + ''' || my_interval || ''' AS ts_end
		FROM (
			SELECT
				FIRST(ts, ts) AS ts
			FROM positions
		
			UNION
		
			SELECT
				LAST(ts, ts) AS ts
			FROM events_sender_position
		)
	)
	
	INSERT INTO events_sender_position(ts, src_call, address_type, aircraft_type, is_stealth, is_notrack, address, software_version, hardware_version, original_address, event)
	SELECT
		ts,
		src_call,
	
		address_type,
		aircraft_type,
		is_stealth,
		is_notrack,
		address,
		software_version,
		hardware_version,
		original_address,
	
		event
	FROM (
		SELECT
			*,
		
			-- event
			-- bit 0: address_type change
			-- bit 1: aircraft_type change
			-- bit 2: is_stealth change
			-- bit 3: is_notrack change
			-- bit 4: address change
			-- bit 5: software_version change
			-- bit 6: hardware_version change
			-- bit 7: original_address change
			0
			+ CASE WHEN address_type_prev IS NOT NULL AND address_type_prev != address_type THEN 1 ELSE 0 END
			+ CASE WHEN aircraft_type_prev IS NOT NULL AND aircraft_type_prev != aircraft_type THEN 2 ELSE 0 END
			+ CASE WHEN is_stealth_prev IS NOT NULL AND is_stealth_prev != is_stealth THEN 4 ELSE 0 END
			+ CASE WHEN is_notrack_prev IS NOT NULL AND is_notrack_prev != is_notrack THEN 8 ELSE 0 END
			+ CASE WHEN address_prev IS NOT NULL AND address_prev != address THEN 16 ELSE 0 END
			+ CASE WHEN software_version_prev IS NOT NULL AND software_version_prev != software_version THEN 32 ELSE 0 END
			+ CASE WHEN hardware_version_prev IS NOT NULL AND hardware_version_prev != hardware_version THEN 64 ELSE 0 END
			+ CASE WHEN original_address_prev IS NOT NULL AND original_address_prev != original_address THEN 128 ELSE 0 END
			AS event,
		
			ROW_NUMBER() OVER (PARTITION BY src_call, address_type, aircraft_type, is_stealth, is_notrack, address, software_version, hardware_version, original_address ORDER BY ts) AS rn
		FROM (
			SELECT
				*,
		
				LAG(address_type) OVER (PARTITION BY src_call ORDER BY ts) AS address_type_prev,
				LAG(aircraft_type) OVER (PARTITION BY src_call ORDER BY ts) AS aircraft_type_prev,
				LAG(is_stealth) OVER (PARTITION BY src_call ORDER BY ts) AS is_stealth_prev,
				LAG(is_notrack) OVER (PARTITION BY src_call ORDER BY ts) AS is_notrack_prev,
				LAG(address) OVER (PARTITION BY src_call ORDER BY ts) AS address_prev,
				LAG(software_version) OVER (PARTITION BY src_call ORDER BY ts) AS software_version_prev,
				LAG(hardware_version) OVER (PARTITION BY src_call ORDER BY ts) AS hardware_version_prev,
				LAG(original_address) OVER (PARTITION BY src_call ORDER BY ts) AS original_address_prev
			FROM (
				SELECT
					ts,
					src_call,
		
					address_type,
					aircraft_type,
					is_stealth,
					is_notrack,
					address,
					FIRST_VALUE(software_version) OVER (PARTITION BY inner_group ORDER BY ts) AS software_version,
					FIRST_VALUE(hardware_version) OVER (PARTITION BY inner_group ORDER BY ts) AS hardware_version,
					FIRST_VALUE(original_address) OVER (PARTITION BY inner_group ORDER BY ts) AS original_address
		
				FROM (
					SELECT	
						*,
				
						SUM(
							CASE
								WHEN software_version IS NOT NULL OR hardware_version IS NOT NULL OR original_address IS NOT NULL
								THEN 1
								ELSE 0
							END
						) OVER (PARTITION BY src_call, address_type, aircraft_type, is_stealth, is_notrack, address ORDER BY ts) AS inner_group				
					FROM (
						SELECT
							ts,
							src_call,
				
							address_type,
							aircraft_type,
							is_stealth,
							is_notrack,
							address,
							software_version,
							hardware_version,
							original_address
						FROM positions
						WHERE
							ts BETWEEN (SELECT ts_start FROM range_to_compute) AND (SELECT ts_end FROM range_to_compute)
							AND src_call NOT LIKE ''RND%''
							AND dst_call IN (''OGFLR'', ''OGNFNT'', ''OGNTRK'')
							AND address IS NOT NULL
			
						UNION
					
						SELECT
							ts,
							src_call,
				
							address_type,
							aircraft_type,
							is_stealth,
							is_notrack,
							address,
							software_version,
							hardware_version,
							original_address
						FROM (
							SELECT
								*,
								ROW_NUMBER() OVER (PARTITION BY src_call ORDER BY ts DESC) AS rn
							FROM events_sender_position
						) AS sq
						WHERE sq.rn = 1
					) AS sq
				) AS sq2
			) AS sq3
		) AS sq4
	) AS sq5
	WHERE
		ts BETWEEN (SELECT ts_start FROM range_to_compute) AND (SELECT ts_end FROM range_to_compute)
		AND (rn = 1 OR event != 0)
	ORDER BY 1
	ON CONFLICT DO NOTHING;
	';

GET DIAGNOSTICS processed_event_rows = ROW_COUNT;
RETURN processed_event_rows;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_events_takeoff(source TEXT, target TEXT, lower TIMESTAMPTZ, upper TIMESTAMPTZ)
RETURNS INTEGER AS $$
DECLARE
	processed_position_rows INTEGER;
BEGIN

EXECUTE '
	WITH positions_selection AS (
		SELECT
			src_call,
			
			LAG(receiver_ts, 2) OVER w AS receiver_ts_pp,
			LAG(receiver_ts, 1) OVER w AS receiver_ts_p,
			receiver_ts,
			LEAD(receiver_ts, 1) OVER w AS receiver_ts_n,
			LEAD(receiver_ts, 2) OVER w AS receiver_ts_nn,
			
			LAG(course, 2) OVER w AS course_pp,
			LAG(course, 1) OVER w AS course_p,
			course,
			LEAD(course, 1) OVER w AS course_n,
			LEAD(course, 2) OVER w AS course_nn,
			
			LAG(speed, 2) OVER w AS speed_pp,
			LAG(speed, 1) OVER w AS speed_p,
			speed,
			LEAD(speed, 1) OVER w AS speed_n,
			LEAD(speed, 2) OVER w AS speed_nn,
			
			LAG(altitude, 2) OVER w AS altitude_pp,
			LAG(altitude, 1) OVER w AS altitude_p,
			altitude,
			LEAD(altitude, 1) OVER w AS altitude_n,
			LEAD(altitude, 2) OVER w AS altitude_nn,
		
			LAG(climb_rate, 2) OVER w AS climb_rate_pp,
			LAG(climb_rate, 1) OVER w AS climb_rate_p,
			climb_rate,
			LEAD(climb_rate, 1) OVER w AS climb_rate_n,
			LEAD(climb_rate, 2) OVER w AS climb_rate_nn,
		
			LAG(turn_rate, 2) OVER w AS turn_rate_pp,
			LAG(turn_rate, 1) OVER w AS turn_rate_p,
			turn_rate,
			LEAD(turn_rate, 1) OVER w AS turn_rate_n,
			LEAD(turn_rate, 2) OVER w AS turn_rate_nn,
		
			LAG(location, 2) OVER w AS location_pp,
			LAG(location, 1) OVER w AS location_p,
			location,
			LEAD(location, 1) OVER w AS location_n,
			LEAD(location, 2) OVER w AS location_nn,

			takeoff_speed / 1.852 AS takeoff_speed,
			landing_speed / 1.852 AS landing_speed
		FROM (
			SELECT
				FIRST_VALUE(src_call) OVER w AS src_call,
				FIRST_VALUE(receiver_ts) OVER w AS receiver_ts,
				FIRST_VALUE(course) OVER w AS course,
				FIRST_VALUE(speed) OVER w AS speed,
				FIRST_VALUE(altitude) OVER w AS altitude,
				FIRST_VALUE(climb_rate) OVER w AS climb_rate,
				FIRST_VALUE(turn_rate) OVER w AS turn_rate,
				FIRST_VALUE(location) OVER w AS location,

				FIRST_VALUE(takeoff_speed) OVER w as takeoff_speed,
				FIRST_VALUE(landing_speed) OVER w as landing_speed
			FROM ' || source || ' AS p
			INNER JOIN (
				SELECT * FROM (
					VALUES
						(1, 55, 40),	-- GLIDER/MOTORGLIDER
						(2, 55, 40),	-- TOWPLANE
						(3, 55, 40),	-- HELICOPTER
						(4, 55, 40),	-- PARACHUTE
						(5, 55, 40),	-- DROPPLANE
						(6, 55, 40),	-- HANGGLIDER
						(7, 55, 40),	-- PARAGLIDER
						(8, 55, 40),	-- PLANE
						(9, 55, 40),	-- JET
						(10, 55, 40),	-- UFO
						(11, 55, 40),	-- BALLOON
						(12, 55, 40),	-- AIRSHIP
						(13, 55, 40),	-- UAV
						(14, 55, 40),	-- GROUND SUPPORT
						(15, 55, 40)	-- STATIC OBJECT
				) AS t (aircraft_type, takeoff_speed, landing_speed)
			) AS speed_limit ON speed_limit.aircraft_type = p.aircraft_type
			WHERE
				ts BETWEEN ''' || lower || ''' AND ''' || upper || '''
				AND dst_call IN (''OGFLR'', ''OGNFNT'', ''OGNTRK'')
			WINDOW w AS (PARTITION BY src_call, receiver_ts ORDER BY COALESCE(error, 0))
		)
		WINDOW w AS (PARTITION BY src_call ORDER BY receiver_ts)
	)

	INSERT INTO ' || target || ' AS t (src_call, receiver_ts, course, altitude, location, event)
	SELECT
		src_call,
		receiver_ts,
		
		course,
		--speed,
		altitude,
		--climb_rate,
		--turn_rate,
		location,

		event
	FROM (
		SELECT
			CASE
				WHEN
						receiver_ts_pp > receiver_ts_p - INTERVAL''30 seconds''
						AND receiver_ts_p > receiver_ts - INTERVAL''30 seconds''
						AND receiver_ts > receiver_ts_n - INTERVAL''30 seconds''
						AND receiver_ts_n > receiver_ts_nn - INTERVAL''30 seconds''
						AND ST_DistanceSphere(location_pp, location_p) < 500
						AND ST_DistanceSphere(location_p, location) < 500
						AND ST_DistanceSphere(location, location_n) < 500
						AND ST_DistanceSphere(location_n, location_nn) < 500
					THEN CASE
						WHEN
							speed_pp < takeoff_speed
							AND speed_p < takeoff_speed
							AND speed > takeoff_speed
							AND speed_n > takeoff_speed
							AND speed_nn > takeoff_speed
						THEN 6
						WHEN
							speed_pp > landing_speed
							AND speed_p > landing_speed
							AND speed < landing_speed
							AND speed_n < landing_speed
							AND speed_nn < landing_speed
						THEN 7
						ELSE NULL
					END
				WHEN
						receiver_ts_pp > receiver_ts_p - INTERVAL''30 seconds''
						AND receiver_ts_p > receiver_ts - INTERVAL''30 seconds''
						AND receiver_ts > receiver_ts_n - INTERVAL''30 seconds''
						AND ST_DistanceSphere(location_pp, location_p) < 500
						AND ST_DistanceSphere(location_p, location) < 500
						AND ST_DistanceSphere(location, location_n) < 500
					THEN CASE
						WHEN
							speed_pp < takeoff_speed
							AND speed_p < takeoff_speed
							AND speed > takeoff_speed
							AND speed_n > takeoff_speed
						THEN 4
						WHEN
							speed_pp > landing_speed
							AND speed_p > landing_speed
							AND speed < landing_speed
							AND speed_n < landing_speed
						THEN 5
						ELSE NULL
					END
				WHEN
						receiver_ts_pp > receiver_ts_p - INTERVAL''30 seconds''
						AND receiver_ts_p > receiver_ts - INTERVAL''30 seconds''
						AND ST_DistanceSphere(location_pp, location_p) < 500
						AND ST_DistanceSphere(location_p, location) < 500
					THEN CASE
						WHEN
							speed_pp < takeoff_speed
							AND speed_p < takeoff_speed
							AND speed > takeoff_speed
						THEN 2
						WHEN
							speed_pp > landing_speed
							AND speed_p > landing_speed
							AND speed < landing_speed
						THEN 3
						ELSE NULL
					END
				WHEN 
						receiver_ts_p > receiver_ts - INTERVAL''30 seconds''
						AND ST_DistanceSphere(location_p, location) < 500
					THEN CASE
						WHEN
							speed_p < takeoff_speed
							AND speed > takeoff_speed
						THEN 0
						WHEN
							speed_p > landing_speed
							AND speed < landing_speed
						THEN 1
						ELSE NULL
					END
				ELSE NULL
			END AS event,
			*
		FROM positions_selection
	)
	WHERE event IS NOT NULL
	ON CONFLICT (src_call, receiver_ts) DO UPDATE
		SET event = CASE WHEN excluded.event > t.event THEN excluded.event ELSE t.event END;
';
	
	GET DIAGNOSTICS processed_position_rows = ROW_COUNT;
	RETURN processed_position_rows;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_events_takeoff(lower TIMESTAMPTZ, upper TIMESTAMPTZ)
RETURNS INTEGER AS $$
DECLARE
	processed_position_rows INTEGER;
BEGIN

EXECUTE '
	SELECT update_events_takeoff(''positions'', ''events_takeoff'', ''' || lower || ''', ''' || upper || ''');
';
	
	GET DIAGNOSTICS processed_position_rows = ROW_COUNT;
	RETURN processed_position_rows;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_takeoffs(lower TIMESTAMPTZ, upper TIMESTAMPTZ)
RETURNS INTEGER AS $$
DECLARE
	processed_position_rows INTEGER;
BEGIN

EXECUTE '
	INSERT INTO takeoffs (receiver_ts, src_call, course, event, airport_name, airport_iso2, airport_tzid)
	SELECT
		receiver_ts,
		src_call,
		course,
		event,
		a.name AS airport_name,
		a.iso2 AS airport_iso2,
		a.tzid AS airport_tzid
	FROM events_takeoff AS e
	CROSS JOIN LATERAL (
		SELECT *
		FROM openaip
		ORDER BY openaip.location <-> e.location
		LIMIT 1
	) AS a
	WHERE
		receiver_ts BETWEEN ''' || lower || ''' AND ''' || upper || '''
		e.altitude*0.3048 BETWEEN a.altitude - 100 AND a.altitude + 200
		AND ST_DistanceSphere(e.location, a.location) < 2500
	ON CONFLICT (src_call, receiver_ts) DO UPDATE
		SET event = excluded.event;
';
	
	GET DIAGNOSTICS processed_position_rows = ROW_COUNT;
	RETURN processed_position_rows;

END;
$$ LANGUAGE plpgsql;


