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
