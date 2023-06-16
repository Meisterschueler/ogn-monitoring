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

		CASE
			WHEN distance IS NULL OR altitude IS NULL OR receiver_ts_jump OR receiver_ts_duplicate THEN -1
			ELSE
				0
				+ CASE WHEN vertical_jump = TRUE OR horizontal_jump = TRUE THEN 1 ELSE 0 END
				+ CASE WHEN vertical_receiver_jump = TRUE OR horizontal_receiver_jump = TRUE THEN 2 ELSE 0 END
				
				+ CASE WHEN vertical_jumps_range > 0 OR horizontal_jumps_range > 0 THEN 4 ELSE 0 END
				+ CASE WHEN vertical_receiver_jumps_range > 0 OR horizontal_receiver_jumps_range > 0 THEN 8 ELSE 0 END
				
				+ CASE WHEN messages_range = 1 THEN 16 ELSE 0 END
				+ CASE WHEN messages_receiver_range = 1 THEN 32 ELSE 0 END

				+ CASE WHEN receivers_confirming_range = 1 THEN 256 ELSE 0 END
				+ CASE WHEN receivers_confirming_point = 1 THEN 512 ELSE 0 END
				+ CASE WHEN receivers_confirming_location = 1 THEN 1024 ELSE 0 END
				+ CASE WHEN receivers_confirming_exact = 1 THEN 2048 ELSE 0 END

				--+ (CASE WHEN receivers_confirming_point >= 3 AND receivers_confirming_range - receivers_confirming_point > 1 THEN 16384 ELSE 0 END)
				--+ (CASE WHEN receivers_confirming_location >= 3 AND receivers_confirming_point - receivers_confirming_location > 1 THEN 32768 ELSE 0 END)
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

			-- point jumps
			vertical_jump_prev OR vertical_jump_next AS vertical_jump,
			horizontal_jump_prev OR horizontal_jump_next AS horizontal_jump,
			vertical_receiver_jump_prev OR vertical_receiver_jump_next AS vertical_receiver_jump,
			horizontal_receiver_jump_prev OR horizontal_receiver_jump_next AS horizontal_receiver_jump,

			-- range jumps
			SUM(CAST(vertical_jump_prev OR vertical_jump_next AS INT)) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS vertical_jumps_range,
			SUM(CAST(horizontal_jump_prev OR horizontal_jump_next AS INT)) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS horizontal_jumps_range,
			SUM(CAST(vertical_receiver_jump_prev OR vertical_receiver_jump_next AS INT)) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS vertical_receiver_jumps_range,
			SUM(CAST(horizontal_receiver_jump_prev OR horizontal_receiver_jump_next AS INT)) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS horizontal_receiver_jumps_range,

			-- messages count over range
			COUNT(*) OVER (PARTITION BY src_call ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS messages_range,
			COUNT(*) OVER (PARTITION BY src_call, receiver ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING) AS messages_receiver_range,

			-- confirmations
			receivers_confirming_range,
			receivers_confirming_point,
			receivers_confirming_location,
			receivers_confirming_exact,

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
					+ 1 AS receivers_confirming_range,

				-- in the same timestamp
				MAX(receiver_rank_point) OVER (PARTITION BY src_call, receiver_ts ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING)
					- MIN(receiver_rank_point) OVER (PARTITION BY src_call, receiver_ts ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING)
					+ 1 AS receivers_confirming_point,

				-- in the same location (2d)
				MAX(receiver_rank_location) OVER (PARTITION BY src_call, receiver_ts, location ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING)
					- MIN(receiver_rank_location) OVER (PARTITION BY src_call, receiver_ts, location ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING)
					+ 1 AS receivers_confirming_location,

				-- with the same altitude (3d)
				MAX(receiver_rank_exact) OVER (PARTITION BY src_call, receiver_ts, location, altitude ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING)
					- MIN(receiver_rank_exact) OVER (PARTITION BY src_call, receiver_ts, location, altitude ORDER BY receiver_ts RANGE BETWEEN INTERVAL ''5 minutes'' PRECEDING AND INTERVAL ''5 minutes'' FOLLOWING)
					+ 1 AS receivers_confirming_exact
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
					DENSE_RANK() OVER (PARTITION BY src_call, receiver_ts, location ORDER BY receiver) AS receiver_rank_location,
					DENSE_RANK() OVER (PARTITION BY src_call, receiver_ts, location, altitude ORDER BY receiver) AS receiver_rank_exact
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
	AND pos.ts = plausibilities.ts;
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
