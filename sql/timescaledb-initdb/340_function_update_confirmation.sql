-- create a function which updates receiver confirmations and returns the proceeded rows
CREATE OR REPLACE FUNCTION update_confirmations()
RETURNS INTEGER AS $$
DECLARE
	processed_position_rows INTEGER;
BEGIN

	WITH confirmations AS (
		SELECT
			ts,
			receiver1,
			receiver2,
			altitude2-altitude1 AS altitude_delta,
			COUNT(*) AS messages
		FROM (
			SELECT
				time_bucket('1 day', ts) AS ts,
				receiver AS receiver1,
				altitude AS altitude1,
				FIRST_VALUE(receiver) OVER (PARTITION BY src_call, receiver_ts ORDER BY receiver) AS receiver2,
				FIRST_VALUE(altitude) OVER (PARTITION BY src_call, receiver_ts ORDER BY receiver) AS altitude2
			FROM positions
			WHERE
				ts > (NOW() - INTERVAL '1 hour')::DATE
				AND error = 0
				AND altitude IS NOT NULL
				AND src_call NOT LIKE 'RND%'
				AND dst_call IN ('APRS', 'OGFLR', 'OGNFNT', 'OGNTRK')
				AND receiver NOT LIKE 'GLIDERN%'
				AND receiver NOT LIKE 'PW%'
		) AS sq
		WHERE receiver1 != receiver2
		GROUP BY 1, 2, 3, 4
	)

	INSERT INTO confirmations_1d (ts, receiver1, receiver2, altitude_delta, messages)
	SELECT * FROM confirmations
	ON CONFLICT (ts, receiver1, receiver2, altitude_delta) DO UPDATE
	SET
		messages = excluded.messages;
	
	GET DIAGNOSTICS processed_position_rows = ROW_COUNT;
	RETURN processed_position_rows;

END;
$$ LANGUAGE plpgsql;
