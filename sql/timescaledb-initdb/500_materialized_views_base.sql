-- aggregate latest messages (positions only) for senders
-- cost: 4s
CREATE MATERIALIZED VIEW senders
AS
SELECT
	src_call,

	array_agg(DISTINCT original_address ORDER BY original_address) FILTER (WHERE original_address IS NOT NULL) AS original_addresses,

	LAST(ts_last, ts) AS ts_first,
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

	0 AS changed,
	SUM(messages) AS messages,
	0 AS buckets_5m,
	SUM(buckets_15m) AS buckets_15m,
	COUNT(*) AS buckets_1d
FROM positions_sender_original_address_1d
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
	
	AVG((r1d.normalized_quality_max - sq.minimum) / (sq.maximum - sq.minimum)) AS relative_quality,
	SUM(sq.buckets_1d) AS buckets_1d,

	SUM(CASE WHEN r1d.normalized_quality_max <= sq.decile_01 THEN 1 ELSE 0 END) AS reports_below_10,
	COUNT(*) AS reports_total
FROM records_1d AS r1d
INNER JOIN (
	SELECT
		ts,
		receiver,
		
		MIN(normalized_quality_max) AS minimum,
		PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY normalized_quality_max) AS decile_01,
		MAX(normalized_quality_max) AS maximum,
	
		COUNT(*) AS buckets_1d
	FROM records_1d
	GROUP BY 1, 2
	HAVING MIN(normalized_quality_max) != MAX(normalized_quality_max)
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
	
	AVG(
		CASE
			WHEN r1d.normalized_quality_max < sq.decile_01 THEN 0.1
			WHEN r1d.normalized_quality_max >= sq.decile_01 AND r1d.normalized_quality_max < sq.decile_02 THEN 0.2
			WHEN r1d.normalized_quality_max >= sq.decile_02 AND r1d.normalized_quality_max < sq.decile_03 THEN 0.3
			WHEN r1d.normalized_quality_max >= sq.decile_03 AND r1d.normalized_quality_max < sq.decile_04 THEN 0.4
			WHEN r1d.normalized_quality_max >= sq.decile_04 AND r1d.normalized_quality_max < sq.decile_05 THEN 0.5
			WHEN r1d.normalized_quality_max >= sq.decile_05 AND r1d.normalized_quality_max < sq.decile_06 THEN 0.6
			WHEN r1d.normalized_quality_max >= sq.decile_06 AND r1d.normalized_quality_max < sq.decile_07 THEN 0.7
			WHEN r1d.normalized_quality_max >= sq.decile_07 AND r1d.normalized_quality_max < sq.decile_08 THEN 0.8
			WHEN r1d.normalized_quality_max >= sq.decile_08 AND r1d.normalized_quality_max < sq.decile_09 THEN 0.9
			ELSE 1.0
		END
	) AS relative_quality,
	SUM(sq.buckets_1d) AS buckets_1d,

	SUM(CASE WHEN r1d.normalized_quality_max < sq.decile_01 THEN 1 ELSE 0 END) AS reports_below_10,
	COUNT(*) AS reports_total
FROM records_1d AS r1d
INNER JOIN (
	SELECT
		ts,
		src_call,

		MIN(normalized_quality_max) AS minimum,
		PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY normalized_quality_max) AS decile_01,
		PERCENTILE_DISC(0.2) WITHIN GROUP (ORDER BY normalized_quality_max) AS decile_02,
		PERCENTILE_DISC(0.3) WITHIN GROUP (ORDER BY normalized_quality_max) AS decile_03,
		PERCENTILE_DISC(0.4) WITHIN GROUP (ORDER BY normalized_quality_max) AS decile_04,
		PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY normalized_quality_max) AS decile_05,
		PERCENTILE_DISC(0.6) WITHIN GROUP (ORDER BY normalized_quality_max) AS decile_06,
		PERCENTILE_DISC(0.7) WITHIN GROUP (ORDER BY normalized_quality_max) AS decile_07,
		PERCENTILE_DISC(0.8) WITHIN GROUP (ORDER BY normalized_quality_max) AS decile_08,
		PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY normalized_quality_max) AS decile_09,
		PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY normalized_quality_max) AS decile_10,
		MAX(normalized_quality_max) AS maximum,
	
		COUNT(*) AS buckets_1d
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
	sq.*,
	a.name AS airport_name,
	a.code AS airport_code,
	a.iso2 AS airport_iso2,
	iso2_to_emoji(a.iso2) AS airport_flag,
	a.location AS airport_location,
	a.altitude AS airport_altitude,
	a.style AS airport_style,
	CASE
		WHEN sq.location IS NOT NULL AND a.location IS NOT NULL THEN ST_DistanceSphere(sq.location, a.location)
		ELSE NULL
	END as airport_distance,
	degrees(ST_Azimuth(sq.location, a.location)) AS airport_radial
FROM (
	SELECT 
		src_call AS src_call,
		original_address AS original_address,
		
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
		COUNT(*) AS buckets_15m,
		
		COUNT(*) OVER (PARTITION BY src_call) AS duplicates
	FROM positions_sender_original_address_1d AS sps1d
	LEFT JOIN flarm_expiry AS fe ON sps1d.software_version = fe.version
	WHERE
		original_address IS NOT NULL
		AND fe.version IS NOT NULL
		AND fe.expiry_date >= sps1d.ts_last
	GROUP BY 1, 2
	HAVING SUM(messages) >= 3 AND COUNT(*) >= 3
) AS sq
CROSS JOIN LATERAL (
	SELECT *
	FROM openaip
	ORDER BY openaip.location <-> sq.location
	LIMIT 1
) AS a
WHERE
	sq.duplicates > 1
ORDER BY 1, 2;
CREATE UNIQUE INDEX duplicates_idx ON duplicates (src_call, original_address);

-- aggregate latest messages (positions and statuses) for receivers
-- cost: 16s
CREATE MATERIALIZED VIEW receivers
AS
WITH positions_sender AS (
	SELECT
		receiver,
		MIN(ts_first) AS ts_first,
		MAX(ts_last) AS ts_last,
		SUM(messages) AS messages
	FROM positions_sender_1d
	GROUP BY 1
),
positions_receiver AS (
	SELECT
		src_call,
		MIN(ts_first) AS ts_first,
		MAX(ts_last) AS ts_last,
		LAST(location, ts) AS location,
		LAST(altitude, ts) AS altitude,
		SUM(messages) AS messages
	FROM positions_receiver_1d
	GROUP BY 1
),
statuses_receiver AS (
	SELECT
		src_call,
		MIN(ts_first) AS ts_first,
		MAX(ts_last) AS ts_last,
		LAST(version, ts) AS version,
		LAST(platform, ts) AS platform,
		SUM(messages) AS messages
	FROM statuses_receiver_1d
	GROUP BY 1
)

SELECT 
	CASE
		WHEN ps.receiver IS NOT NULL THEN ps.receiver
		WHEN pr.src_call IS NOT NULL THEN pr.src_call
		WHEN sr.src_call IS NOT NULL THEN sr.src_call
	END AS src_call,
	
	ps.ts_first AS ts_first_sender,
	ps.ts_last AS ts_last_sender,
	ps.messages AS messages_sender,
	
	pr.ts_first AS ts_first_position,
	pr.ts_last AS ts_last_position,
	pr.location,
	pr.altitude,
	pr.messages AS messages_position,
	
	sr.ts_first AS ts_first_status,
	sr.ts_last AS ts_last_status,
	sr.version,
	sr.platform,
	sr.messages AS messages_status
FROM positions_sender AS ps
FULL OUTER JOIN positions_receiver AS pr ON pr.src_call = ps.receiver
FULL OUTER JOIN statuses_receiver AS sr ON sr.src_call = COALESCE(pr.src_call, ps.receiver);
CREATE UNIQUE INDEX receivers_idx ON receivers (src_call);
