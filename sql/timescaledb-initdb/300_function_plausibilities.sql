-- create a function which updates position plausibilities
CREATE OR REPLACE FUNCTION update_plausibilities(table_name TEXT, lower TIMESTAMP, upper TIMESTAMP)
	RETURNS BIGINT
AS $$
DECLARE
	processed_rows BIGINT;
BEGIN

EXECUTE '

WITH plausibilities AS (
	SELECT
		ts,
		src_call,
		dst_call,
		receiver,

		-- plausibility is a bit-coded value. For this value a time range of +-5min. around the message is analyzed
		--
		--    -1: distance or altitude or normalized_quality is null or receiver_ts is not plausible (jump > 5min relative to ts or duplicate receiver_ts)
		-- bit 0: vertical jump (>300ft/s) (since the altitude of many receivers is not correct this may happen :-( )
		-- bit 1: vertical jump reported from same receiver
		-- bit 2: horizonal jump (>300m/s)
		-- bit 3: horizontal jump reported from same receiver
		-- bit 4: just 1 message received
		-- bit 5: just 1 message received from same receiver
		-- bit 6: no message from other receiver
		-- bit 7: no message from other receiver (same time stamp)
		-- bit 8: no message from other receiver with same location (same time stamp)
		-- bit 9: no message from other receiver with same location (different timestamp)
		-- bit 10: fake distance reported
		-- bit 11: fake distance reported from same receiver
		-- bit 12: fake normalized_quality reported
		-- bit 13: fake normalized_quality reported from same receiver
		CASE
			WHEN distance IS NULL OR altitude IS NULL OR normalized_quality IS NULL OR receiver_ts_jump OR receiver_ts_duplicate THEN -1
			ELSE
				0
				+ CASE WHEN vertical_jumps > 0 THEN 1 ELSE 0 END
				+ CASE WHEN vertical_receiver_jumps > 0 THEN 2 ELSE 0 END

				+ CASE WHEN horizontal_jumps > 0 THEN 4 ELSE 0 END
				+ CASE WHEN horizontal_receiver_jumps > 0 THEN 8 ELSE 0 END

				+ CASE WHEN messages = 1 THEN 16 ELSE 0 END
				+ CASE WHEN messages_receiver = 1 THEN 32 ELSE 0 END

				+ CASE WHEN receivers_confirming = 1 THEN 64 ELSE 0 END
				+ CASE WHEN receivers_confirming_point = 1 THEN 128 ELSE 0 END
				+ CASE WHEN receivers_confirming_location = 1 THEN 256 ELSE 0 END

				+ CASE
					WHEN receivers_confirming_location = 1 AND receivers_confirming_location_count >= 1 THEN 0
					WHEN receivers_confirming_location > 1 AND receivers_confirming_location_count >= 2 THEN 0
					ELSE 512
				  END
				  
				+ CASE WHEN fake_distance > 0 THEN 1024 ELSE 0 END
				+ CASE WHEN fake_distance_receiver > 0 THEN 2048 ELSE 0 END
				
				+ CASE WHEN fake_normalized_quality > 0 THEN 4096 ELSE 0 END
				+ CASE WHEN fake_normalized_quality_receiver > 0 THEN 8192 ELSE 0 END
				
		END AS value
	FROM (
		SELECT
			ts,
			src_call,
			dst_call,
			receiver,
			course,
			speed,
			altitude,
			aircraft_type,
			climb_rate,
			turn_rate,
			error,
			signal_quality,
			receiver_ts,
			bearing,
			distance,
			normalized_quality,
			location,

			-- jumps
			SUM(CAST(vertical_jump_prev OR vertical_jump_next AS INT)) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS vertical_jumps,
			SUM(CAST(horizontal_jump_prev OR horizontal_jump_next AS INT)) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS horizontal_jumps,
			SUM(CAST(vertical_receiver_jump_prev OR vertical_receiver_jump_next AS INT)) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS vertical_receiver_jumps,
			SUM(CAST(horizontal_receiver_jump_prev OR horizontal_receiver_jump_next AS INT)) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS horizontal_receiver_jumps,

			-- messages count
			COUNT(*) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS messages,
			COUNT(*) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS messages_receiver,

			-- confirmations of the current message
			receivers_confirming,
			receivers_confirming_point,
			receivers_confirming_location,

			-- count of message location confirmations
			COUNT(*) FILTER (WHERE receivers_confirming_location > 1) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS receivers_confirming_location_count,

			-- distance plausibility
			COUNT(*) FILTER (WHERE distance >= 1000000) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS fake_distance,
			COUNT(*) FILTER (WHERE distance >= 1000000) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS fake_distance_receiver,

			-- normalized_quality plausibility
			COUNT(*) FILTER (WHERE normalized_quality >= 50) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS fake_normalized_quality,
			COUNT(*) FILTER (WHERE normalized_quality >= 50) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS fake_normalized_quality_receiver,

			-- receiver_ts plausibility
			ABS(EXTRACT(epoch FROM ts - receiver_ts)) > 300 AS receiver_ts_jump,
			COUNT(*) FILTER (WHERE receiver_ts IS NOT NULL) OVER (PARTITION BY src_call, receiver, receiver_ts ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) > 1 AS receiver_ts_duplicate,

			-- limits
			lower_limit,
			upper_limit
		FROM (
			SELECT
				*,

				-- 300 ft/s vertical speed between points is considered as jump
				CASE
					WHEN receiver_ts_delta_prev IS NULL THEN FALSE
					WHEN receiver_ts_delta_prev = 0.0 THEN ABS(altitude_delta_prev) > 300
					WHEN ABS(altitude_delta_prev / receiver_ts_delta_prev) > 300 THEN TRUE
					ELSE FALSE
				END AS vertical_jump_prev,
				CASE
					WHEN receiver_ts_delta_next IS NULL THEN FALSE
					WHEN receiver_ts_delta_next = 0.0 THEN ABS(altitude_delta_next) > 300
					WHEN ABS(altitude_delta_next / receiver_ts_delta_next) > 300 THEN TRUE
					ELSE FALSE
				END AS vertical_jump_next,

				-- 300 m/s horizontal speed between points is considered as jump
				CASE
					WHEN receiver_ts_delta_prev IS NULL THEN FALSE
					WHEN receiver_ts_delta_prev = 0.0 THEN ABS(location_delta_prev) > 300
					WHEN ABS(location_delta_prev / receiver_ts_delta_prev) > 300 THEN TRUE
					ELSE FALSE
				END AS horizontal_jump_prev,
				CASE
					WHEN receiver_ts_delta_next IS NULL THEN FALSE
					WHEN receiver_ts_delta_next = 0.0 THEN ABS(location_delta_next) > 300
					WHEN ABS(location_delta_next / receiver_ts_delta_next) > 300 THEN TRUE
					ELSE FALSE
				END AS horizontal_jump_next,

				-- and also between points OF THE SAME RECEIVER
				CASE
					WHEN receiver_ts_receiver_delta_prev IS NULL THEN FALSE
					WHEN receiver_ts_receiver_delta_prev = 0.0 THEN ABS(altitude_receiver_delta_prev) > 300
					WHEN ABS(altitude_receiver_delta_prev / receiver_ts_receiver_delta_prev) > 300 THEN TRUE
					ELSE FALSE
				END AS vertical_receiver_jump_prev,
				CASE
					WHEN receiver_ts_receiver_delta_next IS NULL THEN FALSE
					WHEN receiver_ts_receiver_delta_next = 0.0 THEN ABS(altitude_receiver_delta_next) > 300
					WHEN ABS(altitude_receiver_delta_next / receiver_ts_receiver_delta_next) > 300 THEN TRUE
					ELSE FALSE
				END AS vertical_receiver_jump_next,
				CASE
					WHEN receiver_ts_receiver_delta_prev IS NULL THEN FALSE
					WHEN receiver_ts_receiver_delta_prev = 0.0 THEN ABS(location_receiver_delta_prev) > 300
					WHEN ABS(location_receiver_delta_prev / receiver_ts_receiver_delta_prev) > 300 THEN TRUE
					ELSE FALSE
				END AS horizontal_receiver_jump_prev,
				CASE
					WHEN receiver_ts_receiver_delta_next IS NULL THEN FALSE
					WHEN receiver_ts_receiver_delta_next = 0.0 THEN ABS(location_receiver_delta_next) > 300
					WHEN ABS(location_receiver_delta_next / receiver_ts_receiver_delta_next) > 300 THEN TRUE
					ELSE FALSE
				END AS horizontal_receiver_jump_next,

				-- RECEIVER CONFIRMATION CALCULATIONS
				-- how many receivers see the sender in the time range (5 min)
				MAX(receiver_rank_range) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING)
					- MIN(receiver_rank_range) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING)
					+ 1 AS receivers_confirming,

				-- in the same timestamp
				MAX(receiver_rank_point) OVER (PARTITION BY src_call, receiver_ts ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING)
					- MIN(receiver_rank_point) OVER (PARTITION BY src_call, receiver_ts ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING)
					+ 1 AS receivers_confirming_point,

				-- with the same location
				MAX(receiver_rank_location) OVER (PARTITION BY src_call, receiver_ts, location ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING)
					- MIN(receiver_rank_location) OVER (PARTITION BY src_call, receiver_ts, location ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING)
					+ 1 AS receivers_confirming_location
			FROM (
				SELECT
					*,

					-- calculate delta between points
					CASE
						WHEN receiver_ts_prev IS NOT NULL THEN EXTRACT(epoch FROM receiver_ts - receiver_ts_prev)
						ELSE NULL
					END AS receiver_ts_delta_prev,
					CASE
						WHEN receiver_ts_next IS NOT NULL THEN EXTRACT(epoch FROM receiver_ts_next - receiver_ts)
						ELSE NULL
					END AS receiver_ts_delta_next,
					CASE
						WHEN altitude_prev IS NOT NULL THEN altitude - altitude_prev
						ELSE NULL
					END AS altitude_delta_prev,
					CASE
						WHEN altitude_next IS NOT NULL THEN altitude_next - altitude
						ELSE NULL
					END AS altitude_delta_next,
					CASE
						WHEN location_prev IS NOT NULL THEN ST_DistanceSphere(location, location_prev)
						ELSE NULL
					END AS location_delta_prev,
					CASE
						WHEN location_next IS NOT NULL THEN ST_DistanceSphere(location_next, location)
						ELSE NULL
					END AS location_delta_next,

					-- and also points OF THE SAME RECEIVER
					CASE
						WHEN receiver_ts_receiver_prev IS NOT NULL THEN EXTRACT(epoch FROM receiver_ts - receiver_ts_receiver_prev)
						ELSE NULL
					END AS receiver_ts_receiver_delta_prev,
					CASE
						WHEN receiver_ts_receiver_next IS NOT NULL THEN EXTRACT(epoch FROM receiver_ts_receiver_next - receiver_ts)
						ELSE NULL
					END AS receiver_ts_receiver_delta_next,
					CASE
						WHEN altitude_receiver_prev IS NOT NULL THEN altitude - altitude_receiver_prev
						ELSE NULL
					END AS altitude_receiver_delta_prev,
					CASE
						WHEN altitude_receiver_next IS NOT NULL THEN altitude_receiver_next - altitude
						ELSE NULL
					END AS altitude_receiver_delta_next,
					CASE
						WHEN location_receiver_prev IS NOT NULL THEN ST_DistanceSphere(location, location_receiver_prev)
						ELSE NULL
					END AS location_receiver_delta_prev,
					CASE
						WHEN location_receiver_next IS NOT NULL THEN ST_DistanceSphere(location_receiver_next, location)
						ELSE NULL
					END AS location_receiver_delta_next,

					-- needed for the receiver confirmation calculations (next step)
					DENSE_RANK() OVER (PARTITION BY src_call ORDER BY receiver) AS receiver_rank_range,
					DENSE_RANK() OVER (PARTITION BY src_call, receiver_ts ORDER BY receiver) AS receiver_rank_point,
					DENSE_RANK() OVER (PARTITION BY src_call, receiver_ts, location ORDER BY receiver) AS receiver_rank_location
				FROM (
					SELECT
						p.*,

						-- check the neighbours
						LAG(receiver_ts) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS receiver_ts_prev,
						LEAD(receiver_ts) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS receiver_ts_next,
						LAG(altitude) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS altitude_prev,
						LEAD(altitude) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS altitude_next,
						LAG(location) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS location_prev,
						LEAD(location) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS location_next,

						-- and the neighbours OF THE SAME RECEIVER
						LAG(receiver_ts) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS receiver_ts_receiver_prev,
						LEAD(receiver_ts) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS receiver_ts_receiver_next,
						LAG(altitude) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS altitude_receiver_prev,
						LEAD(altitude) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS altitude_receiver_next,
						LAG(location) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS location_receiver_prev,
						LEAD(location) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS location_receiver_next,

						sq.*
					FROM positions AS p,
					(
						SELECT
							TIMESTAMP''' || upper || ''' + INTERVAL''5 minutes'' AS upper_buffer,
							TIMESTAMP''' || upper || ''' AS upper_limit,
							TIMESTAMP''' || lower || ''' AS lower_limit,
							TIMESTAMP''' || lower || ''' - INTERVAL''5 minutes'' AS lower_buffer
					) AS sq
					WHERE
						src_call NOT LIKE ''RND%''
						AND dst_call IN (''APRS'', ''OGFLR'', ''OGNFNT'', ''OGNTRK'')
						AND receiver NOT LIKE ''GLIDERN%''
						AND ts BETWEEN lower_buffer AND upper_buffer
				) AS sq2
			) AS sq3
		) AS sq4
	) AS sq5
	WHERE ts BETWEEN lower_limit AND upper_limit
)
	
UPDATE ' || table_name || ' AS pos
SET plausibility = CAST(plausibilities.value AS SMALLINT)
FROM plausibilities
WHERE
	pos.ts BETWEEN TIMESTAMP''' || lower || ''' AND TIMESTAMP ''' || upper || '''
	AND pos.ts = plausibilities.ts
	AND pos.src_call = plausibilities.src_call
	AND pos.dst_call = plausibilities.dst_call
	AND pos.receiver = plausibilities.receiver;
';

	GET DIAGNOSTICS processed_rows = ROW_COUNT;
	
	RETURN processed_rows;

END;
$$ LANGUAGE plpgsql;

-- this function is for small time ranges (< 1h) ...  
CREATE OR REPLACE FUNCTION update_plausibilities(start_time TIMESTAMP, end_time TIMESTAMP)
  RETURNS void
AS $$
DECLARE
  ts TIMESTAMP;
  ts_execution TIMESTAMP;
  start_table TEXT;
  end_table TEXT;
BEGIN
	SELECT '_timescaledb_internal.' || chunk_name
	INTO start_table
	FROM timescaledb_information.chunks
	WHERE
		hypertable_name = 'positions'
		AND start_time BETWEEN range_start AND range_end;

	SELECT '_timescaledb_internal.' || chunk_name
	INTO end_table
	FROM timescaledb_information.chunks
	WHERE
		hypertable_name = 'positions'
		AND end_time BETWEEN range_start AND range_end;

	ts_execution := clock_timestamp();
	PERFORM update_plausibilities(start_table, start_time, end_time);
	RAISE WARNING '%s: update_plausibilities executed. table: %s, start_time: %s, end_time: %s', clock_timestamp() - ts_execution, start_table, start_time, end_time;

	IF start_table != end_table THEN
		ts_execution := clock_timestamp();
		PERFORM update_plausibilities(end_table, start_time, end_time);
		RAISE WARNING '%s: update_plausibilities executed. table: %s, start_time: %s, end_time: %s', clock_timestamp() - ts_execution, end_table, start_time, end_time;
	END IF;
END;
$$
LANGUAGE plpgsql;

-- if you want to update a chunk with big time ranges (>> 1h) ... use this function
CREATE OR REPLACE FUNCTION update_plausibilities_chunks_hourly(table_name TEXT, start_time TIMESTAMP, end_time TIMESTAMP)
  RETURNS void
AS $$
DECLARE
  ts TIMESTAMP;
  ts_start TIMESTAMP;
BEGIN
  ts := start_time;
  
  WHILE ts < end_time LOOP
  	ts_start := clock_timestamp();
    PERFORM update_plausibilities(table_name, ts, ts + INTERVAL '1 hour');
	
	RAISE WARNING '%s: update_plausibilities executed for timestamp: % - %s', clock_timestamp() - ts_start, ts, ts + INTERVAL '1 hour';
	ts := ts + INTERVAL '1 hour';
  END LOOP;
END;
$$
LANGUAGE plpgsql;
