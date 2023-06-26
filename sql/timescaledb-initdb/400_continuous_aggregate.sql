-- sender position statistics
CREATE MATERIALIZED VIEW sender_positions_5m
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('5 minutes', ts) AS ts,
	src_call,
	dst_call,
	receiver,
	plausibility,

	FIRST(ts, ts) AS first_position,
	LAST(ts, ts) AS last_position,
	MIN(distance) AS distance_min,
	MAX(distance) AS distance_max,
	MIN(altitude) AS altitude_min,
	MAX(altitude) AS altitude_max,
	MIN(normalized_quality) AS normalized_quality_min,
	MAX(normalized_quality) AS normalized_quality_max,

	COUNT(*) AS points_total
FROM positions
WHERE
	src_call NOT LIKE 'RND%'
	AND dst_call IN ('APRS', 'OGFLR', 'OGNFNT', 'OGNTRK')
	AND receiver NOT LIKE 'GLIDERN%'
	AND plausibility IS NOT NULL
GROUP BY 1, 2, 3, 4, 5
WITH NO DATA;

CREATE MATERIALIZED VIEW sender_positions_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('1 hour', ts) AS ts,
	src_call,
	dst_call,
	receiver,
	plausibility,

	FIRST(first_position, ts) AS first_position,
	LAST(last_position, ts) AS last_position,
	MIN(distance_min) AS distance_min,
	MAX(distance_max) AS distance_max,
	MIN(altitude_min) AS altitude_min,
	MAX(altitude_max) AS altitude_max,
	MIN(normalized_quality_min) AS normalized_quality_min,
	MAX(normalized_quality_max) AS normalized_quality_max,

	SUM(points_total) AS points_total
FROM sender_positions_5m
GROUP BY 1, 2, 3, 4, 5
WITH NO DATA;

CREATE MATERIALIZED VIEW sender_positions_1d
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	dst_call,
	receiver,
	plausibility,

	FIRST(first_position, ts) AS first_position,
	LAST(last_position, ts) AS last_position,
	MIN(distance_min) AS distance_min,
	MAX(distance_max) AS distance_max,
	MIN(altitude_min) AS altitude_min,
	MAX(altitude_max) AS altitude_max,
	MIN(normalized_quality_min) AS normalized_quality_min,
	MAX(normalized_quality_max) AS normalized_quality_max,

	SUM(points_total) AS points_total
FROM sender_positions_1h
GROUP BY 1, 2, 3, 4, 5
WITH NO DATA;

CREATE MATERIALIZED VIEW quality_statistics_1d
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('1 day', ts) AS ts,	
	receiver,

	AVG(normalized_quality) AS normalized_quality,
	COUNT(DISTINCT src_call) AS senders_count,
	percentile_disc(0.1) WITHIN GROUP (order by normalized_quality) AS percentile_10,
	percentile_disc(0.2) WITHIN GROUP (order by normalized_quality) AS percentile_20,
	percentile_disc(0.3) WITHIN GROUP (order by normalized_quality) AS percentile_30,
	percentile_disc(0.4) WITHIN GROUP (order by normalized_quality) AS percentile_40,
	percentile_disc(0.5) WITHIN GROUP (order by normalized_quality) AS percentile_50,
	percentile_disc(0.6) WITHIN GROUP (order by normalized_quality) AS percentile_60,
	percentile_disc(0.7) WITHIN GROUP (order by normalized_quality) AS percentile_70,
	percentile_disc(0.8) WITHIN GROUP (order by normalized_quality) AS percentile_80,
	percentile_disc(0.9) WITHIN GROUP (order by normalized_quality) AS percentile_90

FROM positions_1d
WHERE normalized_quality IS NOT NULL AND points_motion > 10
GROUP BY time_bucket('1 day', ts), receiver
WITH NO DATA;

-- sender direction statistics
CREATE MATERIALIZED VIEW sender_directions_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('1 hour', ts) AS ts,
	src_call,
	receiver,
	plausibility,
	CAST(((CAST(bearing AS INTEGER) + 15 + 180) % 360) / 30 AS INTEGER) * 30 AS radial,
	CAST(((CAST(bearing AS INTEGER) + 15 - course + 360) % 360) / 30 AS INTEGER) * 30 AS relative_bearing,
	
	MAX(distance) AS distance,
	MAX(normalized_quality) AS normalized_quality,
	
	COUNT(*) AS points_total
FROM positions
WHERE
	src_call NOT LIKE 'RND%'
	AND dst_call IN ('APRS', 'OGFLR', 'OGNFNT', 'OGNTRK')
	AND receiver NOT LIKE 'GLIDERN%'
	AND plausibility IS NOT NULL
	AND bearing IS NOT NULL AND course IS NOT NULL AND COALESCE(speed, 0) >= 5 AND ABS(COALESCE(climb_rate, 0)) < 2000 AND ABS(COALESCE(turn_rate, 0)) * speed < 30
GROUP BY 1, 2, 3, 4, 5, 6
WITH NO DATA;

CREATE MATERIALIZED VIEW sender_directions_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	receiver,
	plausibility,
	radial,
	relative_bearing,
	
	MAX(distance) AS distance,
	MAX(normalized_quality) AS normalized_quality,
	
	SUM(points_total) AS points_total
FROM sender_directions_1h
GROUP BY 1, 2, 3, 4, 5, 6
WITH NO DATA;
