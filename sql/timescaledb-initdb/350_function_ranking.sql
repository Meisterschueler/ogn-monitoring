-- create a function which updates rankings_1d and returns the proceeded rows
CREATE OR REPLACE FUNCTION update_rankings(lower TIMESTAMPTZ, upper TIMESTAMPTZ)
RETURNS INTEGER AS $$
DECLARE
	processed_position_rows INTEGER;
BEGIN

EXECUTE '
INSERT INTO rankings_1d AS r (ts, receiver, src_call, ts_first, ts_last, distance_min, distance_max, altitude_min, altitude_max, normalized_quality_min, normalized_quality_max, online, is_disqualified)
SELECT
	sq.ts,
	sq.receiver,

	sq.src_call,
	sq.ts_first,
	sq.ts_last,
	sq.distance_min,
	sq.distance_max,
	sq.altitude_min,
	sq.altitude_max,
	sq.normalized_quality_min,
	sq.normalized_quality_max,

	sq3.online,

	CASE
		WHEN sq.group_distance_min > 200000 THEN TRUE	-- ignore receivers who see nothing below 200km
		WHEN sq2.position_jumped = TRUE THEN TRUE		-- ignore receivers with moving position
		WHEN sq3.online IS NULL THEN TRUE				-- ignore receivers without online information
		ELSE FALSE
	END AS is_disqualified
FROM (
	SELECT
		r1d.*,

		ROW_NUMBER() OVER (PARTITION BY ts, receiver ORDER BY distance_max DESC),
		MIN(distance_min) OVER (PARTITION BY ts, receiver) AS group_distance_min
	FROM records_1d AS r1d
    WHERE ts BETWEEN ''' || lower || ''' AND ''' || upper || '''
) AS sq
LEFT JOIN (
	SELECT
		time_bucket(''1 day'', ts) AS ts,
		src_call,
		bit_or(event) | b''01''::INTEGER != 0 AS position_jumped
	FROM events_receiver_position
	WHERE ts BETWEEN ''' || lower || ''' AND ''' || upper || '''
	GROUP BY 1, 2
) AS sq2 ON sq.ts = sq2.ts AND sq.receiver = sq2.src_call
LEFT JOIN (
	SELECT
		ts,
		src_call,
		CAST(buckets_5m AS FLOAT) / MAX(buckets_5m) OVER (PARTITION BY ts) AS online
	FROM online_receiver_1d
) AS sq3 ON sq.ts = sq3.ts AND sq.receiver = sq3.src_call
WHERE sq.row_number = 1	
ON CONFLICT (ts, receiver) DO UPDATE
SET
	src_call = excluded.src_call,
	ts_first = excluded.ts_first,
	ts_last = excluded.ts_last,
	distance_min = excluded.distance_min,
	distance_max = excluded.distance_max,
	altitude_min = excluded.altitude_min,
	altitude_max = excluded.altitude_max,
	normalized_quality_min = excluded.normalized_quality_min,
	normalized_quality_max = excluded.normalized_quality_max,
	online = excluded.online,
	is_disqualified = excluded.is_disqualified;
';

	GET DIAGNOSTICS processed_position_rows = ROW_COUNT;
	RETURN processed_position_rows;

END;
$$ LANGUAGE plpgsql;
