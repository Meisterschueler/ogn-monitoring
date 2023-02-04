-- create a function which updates receiver and returns the proceeded rows
CREATE OR REPLACE FUNCTION update_receivers()
RETURNS INTEGER AS $$
DECLARE
	processed_position_rows INTEGER;
	processed_statuses_rows INTEGER;
BEGIN

	-- CTE with the next unproceeded position messages (receiver) (max. range: 1 day, max count: 1000000)
	WITH unproceeded_receiver_positions AS (
		SELECT p.*
		FROM positions AS p
		WHERE
			p.ts BETWEEN
				(SELECT COALESCE(MAX(last_position), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM receivers LIMIT 1)
				AND
				(
					SELECT MAX(sq.ts) FROM (
						SELECT COALESCE(MAX(last_position), TIMESTAMP '1970-01-01 00:00:00 UTC') + INTERVAL '1 day' AS ts FROM receivers
						UNION
						SELECT COALESCE(FIRST(ts, ts), TIMESTAMP '1970-01-01 00:00:00 UTC') + INTERVAL '1 day' AS ts FROM positions
					) AS sq
				)
			AND p.src_call NOT LIKE 'RND%'
			AND p.dst_call IN ('APRS', 'OGNSDR')
			AND p.receiver LIKE 'GLIDERN%'
		ORDER BY ts
		LIMIT 1000000
	)

	-- create/update receivers - last_position and position
	INSERT INTO receivers AS r (name, last_position, location, altitude)
	SELECT
		src_call AS name,
		LAST(ts, ts) AS last_position,
		LAST(location, ts), 
		LAST(altitude, ts) AS altitude
	FROM unproceeded_receiver_positions
	GROUP BY src_call
	ON CONFLICT (name) DO UPDATE
	SET
		last_position = EXCLUDED.last_position,
		location = EXCLUDED.location,
		altitude = EXCLUDED.altitude;
	
	GET DIAGNOSTICS processed_position_rows = ROW_COUNT;
	
		-- CTE with the next unproceeded status messages (receiver) (max. range: 1 day, max count: 1000000)
	WITH unproceeded_receiver_statuses AS (
		SELECT s.*
		FROM statuses AS s
		WHERE
			s.ts BETWEEN
				(SELECT COALESCE(MAX(last_status), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM receivers LIMIT 1)
				AND
				(
					SELECT MAX(sq.ts) FROM (
						SELECT COALESCE(MAX(last_status), TIMESTAMP '1970-01-01 00:00:00 UTC') + INTERVAL '1 day' AS ts FROM receivers
						UNION
						SELECT COALESCE(FIRST(ts, ts), TIMESTAMP '1970-01-01 00:00:00 UTC') + INTERVAL '1 day' AS ts FROM statuses
					) AS sq
				)
			AND s.src_call NOT LIKE 'RND%'
			AND s.dst_call IN ('APRS', 'OGNSDR')
			AND s.receiver LIKE 'GLIDERN%'
		ORDER BY ts
		LIMIT 1000000
	)

	-- create/update receivers - last_status and position
	INSERT INTO receivers AS r (name, last_status, version, platform)
	SELECT
		src_call AS name,
		LAST(ts, ts) AS last_status,
		LAST(version, ts) AS version,
		LAST(platform, ts) AS platform
	FROM unproceeded_receiver_statuses
	GROUP BY src_call
	ON CONFLICT (name) DO UPDATE
	SET
		last_status = EXCLUDED.last_status,
		version = EXCLUDED.version,
		platform = EXCLUDED.platform;
	
	GET DIAGNOSTICS processed_statuses_rows = ROW_COUNT;
	
	RETURN processed_position_rows + processed_statuses_rows;

END;
$$ LANGUAGE plpgsql;