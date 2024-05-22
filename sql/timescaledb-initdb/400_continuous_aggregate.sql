-- position statistics - base for all position based views
CREATE MATERIALIZED VIEW positions_5m
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('5 minutes', ts) AS ts,
	src_call,
	dst_call,
	receiver,
	original_address,
	plausibility,

	FIRST(ts, ts) AS ts_first,
	LAST(ts, ts) AS ts_last,
	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,

	MIN(distance) AS distance_min,
	MAX(distance) AS distance_max,
	MIN(altitude) AS altitude_min,
	MAX(altitude) AS altitude_max,
	MIN(normalized_quality) AS normalized_quality_min,
	MAX(normalized_quality) AS normalized_quality_max,

	COUNT(*) AS messages
FROM positions
WHERE
	src_call NOT LIKE 'RND%'
	AND (
		(dst_call IN ('OGFLR', 'OGNFNT', 'OGNTRK') AND address IS NOT NULL)
		OR (dst_call = 'OGNSDR' OR (dst_call = 'APRS' AND receiver LIKE 'GLIDERN%'))
	)
GROUP BY 1, 2, 3, 4, 5, 6
WITH NO DATA;

CREATE MATERIALIZED VIEW positions_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	dst_call,
	receiver,
	original_address,
	plausibility,

	FIRST(ts_first, ts) AS ts_first,
	LAST(ts_last, ts) AS ts_last,
	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,

	MIN(distance_min) AS distance_min,
	MAX(distance_max) AS distance_max,
	MIN(altitude_min) AS altitude_min,
	MAX(altitude_max) AS altitude_max,
	MIN(normalized_quality_min) AS normalized_quality_min,
	MAX(normalized_quality_max) AS normalized_quality_max,

	SUM(messages) AS messages,
	COUNT(*) AS buckets_5m
FROM positions_5m
GROUP BY 1, 2, 3, 4, 5, 6
WITH NO DATA;

CREATE MATERIALIZED VIEW positions_sender_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT
	time_bucket('15 minutes', ts) AS ts,
	src_call,
	receiver,

	MIN(ts_first) AS ts_first,
	MAX(ts_last) AS ts_last,

	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,
	
	MIN(distance_min) AS distance_min,
	MAX(distance_max) AS distance_max,
	MIN(altitude_min) AS altitude_min,
	MAX(altitude_max) AS altitude_max,
	MIN(normalized_quality_min) AS normalized_quality_min,
	MAX(normalized_quality_max) AS normalized_quality_max,

	SUM(messages) AS messages,
	COUNT(*) AS buckets_5m
FROM positions_5m
WHERE dst_call IN ('OGFLR', 'OGNFNT', 'OGNTRK')
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE MATERIALIZED VIEW positions_sender_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	receiver,

	MIN(ts_first) AS ts_first,
	MAX(ts_last) AS ts_last,

	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,
	
	MIN(distance_min) AS distance_min,
	MAX(distance_max) AS distance_max,
	MIN(altitude_min) AS altitude_min,
	MAX(altitude_max) AS altitude_max,
	MIN(normalized_quality_min) AS normalized_quality_min,
	MAX(normalized_quality_max) AS normalized_quality_max,

	SUM(messages) AS messages,
	SUM(buckets_5m) AS buckets_5m,
	COUNT(*) AS buckets_15m
FROM positions_sender_1d
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE MATERIALIZED VIEW positions_receiver_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT
	time_bucket('15 minutes', ts) AS ts,
	src_call,
	dst_call,
	receiver,

	MIN(ts) AS ts_first,
	MAX(ts) AS ts_last,
	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,

	SUM(messages) AS messages,
	COUNT(*) AS buckets_5m
FROM positions_5m
WHERE
	dst_call IN ('OGNSDR', 'APRS')
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE MATERIALIZED VIEW positions_receiver_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	dst_call,
	receiver,

	MIN(ts) AS ts_first,
	MAX(ts) AS ts_last,
	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,

	SUM(messages) AS messages,
	SUM(buckets_5m) AS buckets_5m,
	COUNT(*) AS buckets_15m
FROM positions_receiver_15m
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE MATERIALIZED VIEW statistics_sender_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT
	time_bucket('15 minutes', ts) AS ts,
	src_call,

	SUM(messages) FILTER (WHERE dst_call = 'OGFLR') AS messages_ogflr,
	SUM(messages) FILTER (WHERE dst_call = 'OGNFNT') AS messages_ognfnt,
	SUM(messages) FILTER (WHERE dst_call = 'OGNTRK') AS messages_ogntrk,
	COUNT(DISTINCT receiver) FILTER (WHERE dst_call = 'OGFLR') AS receivers_ogflr,
	COUNT(DISTINCT receiver) FILTER (WHERE dst_call = 'OGNFNT') AS receivers_ognfnt,
	COUNT(DISTINCT receiver) FILTER (WHERE dst_call = 'OGNTRK') AS receivers_ogntrk,
	MAX(distance_max) AS distance,
	MAX(distance_max) FILTER (WHERE dst_call = 'OGFLR') AS distance_ogflr,
	MAX(distance_max) FILTER (WHERE dst_call = 'OGNFNT') AS distance_ognfnt,
	MAX(distance_max) FILTER (WHERE dst_call = 'OGNTRK') AS distance_ogntrk,
	MAX(distance_max) FILTER (
		WHERE 
			plausibility IS NOT NULL
			AND plausibility != -1
			AND plausibility & b'11110000111110'::INTEGER = 0		-- no fake signal_quality, no fake distance, no singles, no jumps (but altitude jumps...)
			AND (
				plausibility & b'00001000000000'::INTEGER = 0		-- indirect confirmation
				OR plausibility & b'00000111000000'::INTEGER = 0	-- direct confirmation
			)
			AND distance_max IS NOT NULL
			AND normalized_quality_max IS NOT NULL
	) AS distance_confirmed,
	MAX(normalized_quality_max) AS normalized_quality,
	MAX(normalized_quality_max) FILTER (WHERE dst_call = 'OGFLR') AS normalized_quality_ogflr,
	MAX(normalized_quality_max) FILTER (WHERE dst_call = 'OGNFNT') AS normalized_quality_ognfnt,
	MAX(normalized_quality_max) FILTER (WHERE dst_call = 'OGNTRK') AS normalized_quality_ogntrk,
	MAX(normalized_quality_max) FILTER (
		WHERE 
			plausibility IS NOT NULL
			AND plausibility != -1
			AND plausibility & b'11110000111110'::INTEGER = 0		-- no fake signal_quality, no fake distance, no singles, no jumps (but altitude jumps...)
			AND (
				plausibility & b'00001000000000'::INTEGER = 0		-- indirect confirmation
				OR plausibility & b'00000111000000'::INTEGER = 0	-- direct confirmation
			)
			AND distance_max IS NOT NULL
			AND normalized_quality_max IS NOT NULL
	) AS normalized_quality_confirmed,

	SUM(messages) AS messages,
	COUNT(*) AS buckets_5m
FROM positions_5m
WHERE dst_call IN ('OGFLR', 'OGNFNT', 'OGNTRK')
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW statistics_receiver_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT
	time_bucket('15 minutes', ts) AS ts,
	receiver,

	SUM(messages) FILTER (WHERE dst_call = 'OGFLR') AS messages_ogflr,
	SUM(messages) FILTER (WHERE dst_call = 'OGNFNT') AS messages_ognfnt,
	SUM(messages) FILTER (WHERE dst_call = 'OGNTRK') AS messages_ogntrk,
	COUNT(DISTINCT src_call) FILTER (WHERE dst_call = 'OGFLR') AS senders_ogflr,
	COUNT(DISTINCT src_call) FILTER (WHERE dst_call = 'OGNFNT') AS senders_ognfnt,
	COUNT(DISTINCT src_call) FILTER (WHERE dst_call = 'OGNTRK') AS senders_ogntrk,
	MAX(distance_max) AS distance,
	MAX(distance_max) FILTER (WHERE dst_call = 'OGFLR') AS distance_ogflr,
	MAX(distance_max) FILTER (WHERE dst_call = 'OGNFNT') AS distance_ognfnt,
	MAX(distance_max) FILTER (WHERE dst_call = 'OGNTRK') AS distance_ogntrk,
	MAX(distance_max) FILTER (
		WHERE 
			plausibility IS NOT NULL
			AND plausibility != -1
			AND plausibility & b'11110000111110'::INTEGER = 0		-- no fake signal_quality, no fake distance, no singles, no jumps (but altitude jumps...)
			AND (
				plausibility & b'00001000000000'::INTEGER = 0		-- indirect confirmation
				OR plausibility & b'00000111000000'::INTEGER = 0	-- direct confirmation
			)
			AND distance_max IS NOT NULL
			AND normalized_quality_max IS NOT NULL
	) AS distance_confirmed,
	MAX(normalized_quality_max) AS normalized_quality,
	MAX(normalized_quality_max) FILTER (WHERE dst_call = 'OGFLR') AS normalized_quality_ogflr,
	MAX(normalized_quality_max) FILTER (WHERE dst_call = 'OGNFNT') AS normalized_quality_ognfnt,
	MAX(normalized_quality_max) FILTER (WHERE dst_call = 'OGNTRK') AS normalized_quality_ogntrk,
	MAX(normalized_quality_max) FILTER (
		WHERE 
			plausibility IS NOT NULL
			AND plausibility != -1
			AND plausibility & b'11110000111110'::INTEGER = 0		-- no fake signal_quality, no fake distance, no singles, no jumps (but altitude jumps...)
			AND (
				plausibility & b'00001000000000'::INTEGER = 0		-- indirect confirmation
				OR plausibility & b'00000111000000'::INTEGER = 0	-- direct confirmation
			)
			AND distance_max IS NOT NULL
			AND normalized_quality_max IS NOT NULL
	) AS normalized_quality_confirmed,

	SUM(messages) AS messages,
	COUNT(*) AS buckets_5m
FROM positions_5m
WHERE dst_call IN ('OGFLR', 'OGNFNT', 'OGNTRK')
GROUP BY 1, 2
WITH NO DATA;


-- direction statistics (sender -> receiver) for polar diagram
CREATE MATERIALIZED VIEW directions_1h
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('1 hour', ts) AS ts,
	src_call,
	receiver,
	CAST(((CAST(bearing AS INTEGER) + 15 + 180) % 360) / 30 AS INTEGER) * 30 AS radial,
	CAST(((CAST(bearing AS INTEGER) + 15 - course + 360) % 360) / 30 AS INTEGER) * 30 AS relative_bearing,
	
	MAX(distance) AS distance,
	MAX(normalized_quality) AS normalized_quality,
	
	COUNT(*) AS messages
FROM positions
WHERE
	src_call NOT LIKE 'RND%'
	AND dst_call IN ('OGFLR', 'OGNFNT', 'OGNTRK')
	AND address IS NOT NULL
	AND bearing IS NOT NULL AND distance IS NOT NULL and normalized_quality IS NOT NULL	
	AND plausibility IS NOT NULL AND plausibility != -1
	AND plausibility & b'11110000000000'::INTEGER = 0	-- no fake signal_quality, no fake distance
GROUP BY 1, 2, 3, 4, 5
WITH NO DATA;


-- for duplicate recognition
CREATE MATERIALIZED VIEW positions_sender_original_address_15m
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('15 minutes', ts) AS ts,
	src_call,
	original_address,
	
	LAST(ts, ts) AS ts_last,
	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,
	
	LAST(address_type, ts) AS address_type,
	LAST(aircraft_type, ts) AS aircraft_type,
	LAST(is_stealth, ts) AS is_stealth,
	LAST(is_notrack, ts) AS is_notrack,
	LAST(address, ts) AS address,
	LAST(software_version, ts) FILTER (WHERE software_version IS NOT NULL) AS software_version,
	LAST(hardware_version, ts) FILTER (WHERE software_version IS NOT NULL) AS hardware_version,

	COUNT(*) AS messages
FROM positions
WHERE
	src_call NOT LIKE 'RND%'
	AND dst_call IN ('OGFLR', 'OGNFNT', 'OGNTRK')
	AND address IS NOT NULL
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE MATERIALIZED VIEW positions_sender_original_address_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	original_address,
	
	LAST(ts_last, ts) AS ts_last,
	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,
	
	LAST(address_type, ts) AS address_type,
	LAST(aircraft_type, ts) AS aircraft_type,
	LAST(is_stealth, ts) AS is_stealth,
	LAST(is_notrack, ts) AS is_notrack,
	LAST(address, ts) AS address,
	LAST(software_version, ts) FILTER (WHERE software_version IS NOT NULL) AS software_version,
	LAST(hardware_version, ts) FILTER (WHERE hardware_version IS NOT NULL) AS hardware_version,

	SUM(messages) AS messages,
	COUNT(*) AS buckets_15m
FROM positions_sender_original_address_15m
GROUP BY 1, 2, 3
WITH NO DATA;


-- aggregated statuses
CREATE MATERIALIZED VIEW statuses_15m
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	dst_call,
	receiver,

	FIRST(ts, ts) AS ts_first,
	LAST(ts, ts) AS ts_last,
	LAST(version, ts) AS version,
	LAST(platform, ts) AS platform,

	COUNT(*) AS messages
FROM statuses
WHERE
	src_call NOT LIKE 'RND%'
	AND (
		(dst_call IN ('OGFLR', 'OGNFNT', 'OGNTRK') OR (dst_call = 'APRS' AND receiver NOT LIKE 'GLIDERN%'))
		OR (dst_call = 'OGNSDR' OR (dst_call = 'APRS' AND receiver LIKE 'GLIDERN%'))
	)
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE MATERIALIZED VIEW statuses_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	dst_call,
	receiver,

	FIRST(ts, ts) AS ts_first,
	LAST(ts, ts) AS ts_last,
	LAST(version, ts) AS version,
	LAST(platform, ts) AS platform,

	SUM(messages) AS messages,
	COUNT(*) AS buckets_15m
FROM statuses_15m
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE MATERIALIZED VIEW statuses_sender_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	dst_call,
	receiver,

	FIRST(ts, ts) AS ts_first,
	LAST(ts, ts) AS ts_last,
	LAST(version, ts) AS version,
	LAST(platform, ts) AS platform,

	SUM(messages) AS messages,
	COUNT(*) AS buckets_15m
FROM statuses_15m
WHERE
	dst_call IN ('OGFLR', 'OGNFNT', 'OGNTRK')
	OR (dst_call = 'APRS' AND receiver NOT LIKE 'GLIDERN%')
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE MATERIALIZED VIEW statuses_receiver_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	dst_call,
	receiver,

	FIRST(ts, ts) AS ts_first,
	LAST(ts, ts) AS ts_last,
	LAST(version, ts) AS version,
	LAST(platform, ts) AS platform,

	SUM(messages) AS messages,
	COUNT(*) AS buckets_15m
FROM statuses_15m
WHERE
	dst_call = 'OGNSDR'	
	OR (dst_call = 'APRS' AND receiver LIKE 'GLIDERN%')
GROUP BY 1, 2, 3, 4
WITH NO DATA;

		

CREATE MATERIALIZED VIEW statistics_dst_call_15m
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('15 minutes', ts) AS ts,
	dst_call,
	receiver,

	COUNT(*) AS messages
FROM positions
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE MATERIALIZED VIEW online_receiver_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,

	COUNT(*) AS messages,
	COUNT(DISTINCT time_bucket('5 minutes', ts)) AS buckets_5m,
	AVG(ts - receiver_ts) AS latency
FROM statuses
WHERE
	dst_call = 'OGNSDR'
	OR (dst_call = 'APRS' AND receiver LIKE 'GLIDERN%')
GROUP BY 1, 2
WITH NO DATA;