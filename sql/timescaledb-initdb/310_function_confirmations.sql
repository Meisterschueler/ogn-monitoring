-- create a function which updates receiver confirmations and returns the proceeded rows
CREATE OR REPLACE FUNCTION update_confirmations(mydate DATE)
RETURNS INTEGER AS $$
DECLARE
	processed_position_rows INTEGER;
BEGIN

EXECUTE '
	WITH confirmations AS (
		SELECT
			ts,
			receiver1,
			receiver2,
			altitude2-altitude1 AS altitude_delta,
			COUNT(*) AS messages
		FROM (
			SELECT
				time_bucket(''1 day'', receiver_ts) AS ts,
				receiver AS receiver1,
				altitude AS altitude1,
				location AS location1,
				FIRST_VALUE(receiver) OVER (PARTITION BY src_call, receiver_ts ORDER BY receiver) AS receiver2,
				FIRST_VALUE(altitude) OVER (PARTITION BY src_call, receiver_ts ORDER BY receiver) AS altitude2,
				FIRST_VALUE(location) OVER (PARTITION BY src_call, receiver_ts ORDER BY receiver) AS location2
			FROM positions
			WHERE
				ts BETWEEN DATE''' || mydate ||''' - INTERVAL''5 minutes'' AND DATE''' || mydate || ''' + INTERVAL''24 hours 5 minutes''
				AND COALESCE(error, 0) = 0
				AND altitude IS NOT NULL
				AND location IS NOT NULL
				AND src_call NOT LIKE ''RND%''
				AND dst_call IN (''OGFLR'', ''OGNFNT'', ''OGNTRK'')
		) AS sq
		WHERE
			receiver1 != receiver2
			AND location1 = location2
			AND ts::date = ''' || mydate || '''
		GROUP BY 1, 2, 3, 4
	)

	INSERT INTO confirmations_1d (ts, receiver1, receiver2, altitude_delta, messages)
	SELECT * FROM confirmations
	ON CONFLICT (ts, receiver1, receiver2, altitude_delta) DO UPDATE
	SET
		messages = excluded.messages;
';
	
	GET DIAGNOSTICS processed_position_rows = ROW_COUNT;
	RETURN processed_position_rows;

END;
$$ LANGUAGE plpgsql;
