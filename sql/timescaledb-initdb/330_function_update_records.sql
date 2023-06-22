-- create a function which updates records and returns the proceeded rows
CREATE OR REPLACE FUNCTION update_records()
RETURNS INTEGER AS $$
DECLARE
	processed_position_rows INTEGER;
BEGIN

	WITH records AS (
		SELECT
			sq.ts,
			sq.receiver,

			FIRST(p.distance, p.ts) AS distance,
			FIRST(p.ts, p.ts) AS distance_ts,
			FIRST(p.src_call, p.ts) AS distance_src_call

		FROM positions AS p
		INNER JOIN (
			SELECT
				time_bucket('1 day', ts) AS ts,
				receiver,

				MAX(distance) AS distance
			FROM positions
			WHERE
				ts > (NOW() - INTERVAL '1 hour')::DATE
				AND distance IS NOT NULL
				AND dst_call IN ('APRS', 'OGFLR')
				AND (
					(
						ts > '2023-06-19 08:00:00'
						AND plausibility & b'110000111111'::int = 0	-- no jumps, no singles, no fakes
						AND (
							   plausibility & b'000111000000'::int = 0	-- direct confirmation
							OR plausibility & b'001000000000'::int = 0	-- indirect confirmation
						)
					) OR (
						ts < '2023-06-19 08:00:00'
						AND plausibility = 0
						AND distance < 640000
						AND normalized_quality < 50
					)
				)
			GROUP BY 1, 2
			HAVING MIN(distance) <= 200000	-- ignore 'records' from receivers who dont see anything below 200km
		) AS sq ON p.receiver = sq.receiver AND p.distance = sq.distance
		WHERE p.ts > (NOW() - INTERVAL '1 hour')::DATE
		GROUP BY 1, 2
	)

	INSERT INTO records_1d (ts, receiver, distance, distance_ts, distance_src_call)
	SELECT * FROM records
	ON CONFLICT (ts, receiver) DO UPDATE
	SET
		distance = excluded.distance,
		distance_ts = excluded.distance_ts,
		distance_src_call = excluded.distance_src_call;
	
	GET DIAGNOSTICS processed_position_rows = ROW_COUNT;
	RETURN processed_position_rows;

END;
$$ LANGUAGE plpgsql;
