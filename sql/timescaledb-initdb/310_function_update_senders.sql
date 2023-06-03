-- create a function which updates sender and returns the proceeded rows
CREATE OR REPLACE FUNCTION update_senders()
RETURNS INTEGER AS $$
DECLARE
	processed_position_rows INTEGER;
BEGIN

	-- CTE with the next unproceeded position messages (sender) (max. range: 1 day, max count: 1000000)
	WITH unproceeded_sender_positions AS (
		SELECT p.*
		FROM positions AS p
		WHERE
			p.ts BETWEEN
				(SELECT COALESCE(MAX(last_position), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM senders LIMIT 1)
				AND
				(
					SELECT MAX(sq.ts) FROM (
						SELECT COALESCE(MAX(last_position), TIMESTAMP '1970-01-01 00:00:00 UTC') + INTERVAL '1 day' AS ts FROM senders
						UNION
						SELECT COALESCE(FIRST(ts, ts), TIMESTAMP '1970-01-01 00:00:00 UTC') + INTERVAL '1 day' AS ts FROM positions
					) AS sq
				)
			AND p.src_call NOT LIKE 'RND%'
			AND p.dst_call IN ('APRS', 'OGFLR', 'OGNFNT', 'OGNTRK')
			AND p.receiver NOT LIKE 'GLIDERN%'
		ORDER BY ts
		LIMIT 1000000
	)
	
	-- create/update senders
	INSERT INTO senders AS s (name, original_address, last_position, location, altitude, address_type, aircraft_type, is_stealth, is_notrack, address, software_version, hardware_version)
	SELECT
		src_call AS name,
		original_address,
		MAX(ts) AS last_position,

		LAST(location, ts) FILTER (WHERE location IS NOT NULL) AS location,
		LAST(altitude, ts) FILTER (WHERE altitude IS NOT NULL) AS altitude,

		LAST(address_type, ts) FILTER (WHERE address_type IS NOT NULL) AS address_type,
		LAST(aircraft_type, ts) FILTER (WHERE aircraft_type IS NOT NULL) AS aircraft_type,
		LAST(is_stealth, ts) FILTER (WHERE is_stealth IS NOT NULL) AS is_stealth,
		LAST(is_notrack, ts) FILTER (WHERE is_notrack IS NOT NULL) AS is_notrack,
		LAST(address, ts) FILTER (WHERE address IS NOT NULL) AS address,
		LAST(software_version, ts) FILTER (WHERE software_version IS NOT NULL) AS software_version,
		LAST(hardware_version, ts) FILTER (WHERE hardware_version IS NOT NULL) AS hardware_version
	FROM unproceeded_sender_positions
	GROUP BY src_call, original_address
	ON CONFLICT (name, original_address) DO UPDATE
	SET
		last_position = EXCLUDED.last_position,

		location = CASE EXCLUDED.location IS NOT NULL WHEN TRUE THEN EXCLUDED.location ELSE s.location END,
		altitude = CASE EXCLUDED.altitude IS NOT NULL WHEN TRUE THEN EXCLUDED.altitude ELSE s.altitude END,

		address_type = CASE EXCLUDED.address_type IS NOT NULL WHEN TRUE THEN EXCLUDED.address_type ELSE s.address_type END,
		aircraft_type = CASE EXCLUDED.aircraft_type IS NOT NULL WHEN TRUE THEN EXCLUDED.aircraft_type ELSE s.aircraft_type END,
		is_stealth = CASE EXCLUDED.is_stealth IS NOT NULL WHEN TRUE THEN EXCLUDED.is_stealth ELSE s.is_stealth END,
		is_notrack = CASE EXCLUDED.is_notrack IS NOT NULL WHEN TRUE THEN EXCLUDED.is_notrack ELSE s.is_notrack END,
		address = CASE EXCLUDED.address IS NOT NULL WHEN TRUE THEN EXCLUDED.address ELSE s.address END,
		software_version = CASE EXCLUDED.software_version IS NOT NULL WHEN TRUE THEN EXCLUDED.software_version ELSE s.software_version END,
		hardware_version = CASE EXCLUDED.hardware_version IS NOT NULL WHEN TRUE THEN EXCLUDED.hardware_version ELSE s.hardware_version END;

	GET DIAGNOSTICS processed_position_rows = ROW_COUNT;
	RETURN processed_position_rows;

END;
$$ LANGUAGE plpgsql;