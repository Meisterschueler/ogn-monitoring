-- aggregate latest messages (positions only) for senders
-- cost: 4s
CREATE MATERIALIZED VIEW senders
AS
SELECT
	src_call,

	LAST(original_address, ts) AS original_address,

	FIRST(ts_first, ts) AS ts_first,
	LAST(ts_last, ts) AS ts_last,
	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,

	LAST(address_type, ts) FILTER (WHERE address_type IS NOT NULL) AS address_type,
	LAST(aircraft_type, ts) FILTER (WHERE aircraft_type IS NOT NULL) AS aircraft_type,
	LAST(is_stealth, ts) FILTER (WHERE is_stealth IS NOT NULL) AS is_stealth,
	LAST(is_notrack, ts) FILTER (WHERE is_notrack IS NOT NULL) AS is_notrack,
	LAST(address, ts) FILTER (WHERE address IS NOT NULL) AS address,
	LAST(software_version, ts) FILTER (WHERE software_version IS NOT NULL) AS software_version,
	LAST(hardware_version, ts) FILTER (WHERE hardware_version IS NOT NULL) AS hardware_version,

	BIT_OR(changed)
	| CASE WHEN MIN(address_type) FILTER (WHERE address_type IS NOT NULL) != MAX(address_type) FILTER (WHERE address_type IS NOT NULL) THEN 1 ELSE 0 END
	| CASE WHEN MIN(aircraft_type) FILTER (WHERE aircraft_type IS NOT NULL) != MAX(aircraft_type) FILTER (WHERE aircraft_type IS NOT NULL) THEN 2 ELSE 0 END
	| CASE WHEN MIN(CAST(is_stealth AS INTEGER)) FILTER (WHERE is_stealth IS NOT NULL) != MAX(CAST(is_stealth AS INTEGER)) FILTER (WHERE is_stealth IS NOT NULL) THEN 4 ELSE 0 END
	| CASE WHEN MIN(CAST(is_notrack AS INTEGER)) FILTER (WHERE is_notrack IS NOT NULL) != MAX(CAST(is_notrack AS INTEGER)) FILTER (WHERE is_notrack IS NOT NULL) THEN 8 ELSE 0 END
	| CASE WHEN MIN(address) FILTER (WHERE address IS NOT NULL) != MAX(address) FILTER (WHERE address IS NOT NULL) THEN 16 ELSE 0 END
	| CASE WHEN MIN(software_version) FILTER (WHERE software_version IS NOT NULL) != MAX(software_version) FILTER (WHERE software_version IS NOT NULL) THEN 32 ELSE 0 END
	| CASE WHEN MIN(hardware_version) FILTER (WHERE hardware_version IS NOT NULL) != MAX(hardware_version) FILTER (WHERE hardware_version IS NOT NULL) THEN 64 ELSE 0 END
	AS changed,

	SUM(messages) AS messages,
	SUM(buckets_5m) AS buckets_5m,
	SUM(buckets_15m) AS buckets_15m,
	COUNT(*) AS buckets_1d
FROM sender_position_states_1d
GROUP BY 1
ORDER BY 1;
CREATE UNIQUE INDEX senders_idx ON senders (src_call);

-- Create normalized sender qualities
-- cost: 4s
CREATE MATERIALIZED VIEW sender_relative_qualities
AS
SELECT
	r1d.ts,
	r1d.src_call,
	
	AVG(r1d.normalized_quality_max - sq.normalized_quality) AS relative_quality,
	SUM(sq.buckets_1d) AS buckets_1d,

	SUM(CASE WHEN r1d.normalized_quality_max < sq.percentile_10 THEN 1 ELSE 0 END) AS reports_below_10,
	COUNT(*) AS reports_total
FROM records_1d AS r1d
INNER JOIN (
	SELECT
		ts,
		receiver,

		AVG(normalized_quality_max) AS normalized_quality,
		COUNT(*) AS buckets_1d,	
		
		PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY normalized_quality_max) AS percentile_10
	FROM records_1d
	GROUP BY 1, 2
) AS sq ON r1d.ts = sq.ts AND r1d.receiver = sq.receiver
GROUP BY 1, 2
ORDER BY 1, 2;
CREATE UNIQUE INDEX sender_relative_qualities_idx ON sender_relative_qualities (ts, src_call);

-- Create normalized receiver qualities
-- cost: 3s
CREATE MATERIALIZED VIEW receiver_relative_qualities
AS
SELECT
	r1d.ts,
	r1d.receiver,
	
	AVG(r1d.normalized_quality_max - sq.normalized_quality) AS relative_quality,
	SUM(sq.buckets_1d) AS buckets_1d,

	SUM(CASE WHEN r1d.normalized_quality_max < sq.percentile_10 THEN 1 ELSE 0 END) AS reports_below_10,
	COUNT(*) AS reports_total
FROM records_1d AS r1d
INNER JOIN (
	SELECT
		ts,
		src_call,

		AVG(normalized_quality_max) AS normalized_quality,
		COUNT(*) AS buckets_1d,	
		
		PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY normalized_quality_max) AS percentile_10
	FROM records_1d
	GROUP BY 1, 2
) AS sq ON r1d.ts = sq.ts AND r1d.src_call = sq.src_call
GROUP BY 1, 2
ORDER BY 1, 2;
CREATE UNIQUE INDEX receiver_relative_qualities_idx ON receiver_relative_qualities (ts, receiver);

-- get sender duplicates
-- cost: 3s
CREATE MATERIALIZED VIEW duplicates
AS
SELECT
	sq.*
FROM (
	SELECT 
		src_call AS src_call,
		original_address AS original_address,
		
		LAST(ts_first, ts) AS ts_first,
		LAST(ts_last, ts) AS ts_last,
		LAST(location, ts) AS location,
		LAST(altitude, ts) AS altitude,
		LAST(address_type, ts) AS address_type,
		LAST(aircraft_type, ts) AS aircraft_type,
		LAST(is_stealth, ts) AS is_stealth,
		LAST(is_notrack, ts) AS is_notrack,
		LAST(address, ts) AS address,
		LAST(software_version, ts) AS software_version,
		LAST(hardware_version, ts) AS hardware_version,
		
		SUM(messages) AS messages,
		SUM(buckets_5m) AS buckets_5m,
		SUM(buckets_15m) AS buckets_15m,
		
		COUNT(*) OVER (PARTITION BY src_call) AS duplicates
	FROM sender_position_states_1d AS sps1d
	LEFT JOIN flarm_expiry AS fe ON sps1d.software_version = fe.version
	WHERE
		original_address IS NOT NULL
		AND fe.version IS NOT NULL
		AND fe.expiry_date >= sps1d.ts_last
	GROUP BY 1, 2
	HAVING SUM(messages) >= 3 AND SUM(buckets_5m) >= 3 AND SUM(buckets_15m) >= 3
) AS sq
WHERE
	sq.duplicates > 1
ORDER BY 1, 2;
CREATE UNIQUE INDEX duplicates_idx ON duplicates (src_call, original_address);

-- aggregate latest messages (positions and statuses) for receivers
-- cost: 2s
CREATE MATERIALIZED VIEW receivers
AS
SELECT
	src_call,

	MIN(ts_first) AS ts_first,
	MAX(ts_last) AS ts_last,
	MIN(location) FILTER (WHERE location IS NOT NULL) AS location,
	MIN(altitude) FILTER (WHERE altitude IS NOT NULL) AS altitude,
	MIN(version) FILTER (WHERE version IS NOT NULL) AS version,
	MIN(platform) FILTER (WHERE platform IS NOT NULL) AS platform,

	-- changes
	-- bit 0: location
	-- bit 1: altitude
	-- bit 2: version
	-- bit 3: platform
	BIT_OR(changed) AS changed,

	SUM(messages) AS messages,
	SUM(buckets_15m) AS buckets_15m,
	SUM(buckets_1d) AS buckets_1d
FROM (
	SELECT
		src_call,

		FIRST(ts_first, ts) AS ts_first,
		LAST(ts_last, ts) AS ts_last,
		LAST(location, ts) AS location,
		LAST(altitude, ts) AS altitude,
		NULL AS version,
		NULL AS platform,

		BIT_OR(changed)
		| CASE WHEN MIN(location) != MAX(location) THEN 1 ELSE 0 END
		| CASE WHEN MIN(altitude) != MAX(altitude) THEN 2 ELSE 0 END
		AS changed,

		SUM(messages) AS messages,
		SUM(buckets_15m) AS buckets_15m,
		COUNT(*) AS buckets_1d
	FROM receiver_position_states_1d
	GROUP BY 1

	UNION

	SELECT
		src_call,

		FIRST(ts_first, ts) AS ts_first,
		LAST(ts_last, ts) AS ts_last,
		NULL AS location,
		NULL AS altitude,
		LAST(version, ts) AS version,
		LAST(platform, ts) AS platform,

		(
			BIT_OR(changed)
			| CASE WHEN MIN(version) FILTER (WHERE version IS NOT NULL) != MAX(version) FILTER (WHERE version IS NOT NULL) THEN 1 ELSE 0 END
			| CASE WHEN MIN(platform) FILTER (WHERE platform IS NOT NULL) != MAX(platform) FILTER (WHERE platform IS NOT NULL) THEN 2 ELSE 0 END
		) << 2
		AS changed,

		SUM(messages) AS messages,
		SUM(buckets_15m) AS buckets_15m,
		COUNT(*) AS buckets_1d
	FROM receiver_status_states_1d
	GROUP BY 1
) AS sq
GROUP BY 1
ORDER BY 1, 2;
CREATE UNIQUE INDEX receivers_idx ON receivers (src_call);