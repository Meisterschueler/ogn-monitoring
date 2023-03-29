-- 5m statistics
CREATE MATERIALIZED VIEW positions_5m
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('5 minutes', ts) AS ts,
	src_call,
	receiver,
	
	CASE state = 3
		WHEN FALSE THEN NULL
		ELSE CAST(((CAST(bearing AS INTEGER) + 15 + 180) % 360) / 30 AS INTEGER) * 30
	END AS radial,
	CASE state = 3
		WHEN FALSE THEN NULL
		ELSE CAST(((CAST(bearing AS INTEGER) + 15 - course + 360) % 360) / 30 AS INTEGER) * 30
	END AS relative_bearing,
	
	FIRST(ts, ts) AS first_position,
	LAST(ts, ts) AS last_position,
	MAX(normalized_quality) FILTER (WHERE state IN ('MOTION', 'STATIC')) AS normalized_quality,
	MAX(distance) FILTER (WHERE state IN ('MOTION', 'STATIC')) AS distance,
	MIN(altitude) FILTER (WHERE state IN ('MOTION', 'STATIC') AND altitude IS NOT NULL) AS altitude_min,
	MAX(altitude) FILTER (WHERE state IN ('MOTION', 'STATIC') AND altitude IS NOT NULL) AS altitude_max,
	
	COUNT(*) FILTER (WHERE state = 'MOTION') AS points_motion,
	COUNT(*) FILTER (WHERE state = 'STATIC') AS points_static,
	COUNT(*) FILTER (WHERE state = 'FAKE') AS points_fake,
	COUNT(*) FILTER (WHERE state = 'ERROR') AS points_error,
	COUNT(*) AS points_total
FROM (
	SELECT
		*,
		CASE
			WHEN COALESCE(error, 0) <= 5 AND COALESCE(normalized_quality, 0) <= 50 AND COALESCE(distance, 0) <= 640000 AND bearing IS NOT NULL AND course IS NOT NULL AND COALESCE(speed, 0) >= 5 THEN 'MOTION'
			WHEN COALESCE(error, 0) <= 5 AND COALESCE(normalized_quality, 0) <= 50 AND COALESCE(distance, 0) <= 640000 AND (bearing IS NULL OR course IS NULL OR COALESCE(speed, 0) < 5) THEN 'STATIC'
			WHEN COALESCE(error, 0) <= 5 AND (COALESCE(normalized_quality, 0) > 50 OR COALESCE(distance, 0) > 640000) THEN 'FAKE'
			WHEN COALESCE(error, 0) > 5 THEN 'ERROR'
			ELSE 'UNKNOWN'
		END AS state
	FROM positions
) AS sq
WHERE
	src_call NOT LIKE 'RND%'
	AND dst_call IN ('APRS', 'OGFLR', 'OGNFNT', 'OGNTRK')
	AND receiver NOT LIKE 'GLIDERN%'
GROUP BY time_bucket('5 minutes', ts), src_call, receiver, radial, relative_bearing
WITH NO DATA;

-- ranking statistics 5m
CREATE MATERIALIZED VIEW ranking_statistics_5m
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('5 minutes', ts) AS ts,
	src_call,
	receiver,

	FIRST(first_position, ts) AS first_position,
	LAST(last_position, ts) AS last_position,
	MAX(normalized_quality) AS normalized_quality,
	MAX(distance) AS distance,
	MIN(altitude_min) AS altitude_min,
	MAX(altitude_max) AS altitude_max,

	SUM(points_motion) AS points_motion,
	SUM(points_static) AS points_static,
	SUM(points_fake) AS points_fake,
	SUM(points_error) AS points_error,
	SUM(points_total) AS points_total

FROM positions_5m AS s5m
GROUP BY time_bucket('5 minutes', ts), src_call, receiver
WITH NO DATA;

CREATE MATERIALIZED VIEW ranking_statistics_1h
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('1 hour', ts) AS ts,
	src_call,
	receiver,

	FIRST(first_position, ts) AS first_position,
	LAST(last_position, ts) AS last_position,
	MAX(normalized_quality) AS normalized_quality,
	MAX(distance) AS distance,
	MIN(altitude_min) AS altitude_min,
	MAX(altitude_max) AS altitude_max,

	SUM(points_motion) AS points_motion,
	SUM(points_static) AS points_static,
	SUM(points_fake) AS points_fake,
	SUM(points_error) AS points_error,
	SUM(points_total) AS points_total

FROM ranking_statistics_5m AS rs5m
GROUP BY time_bucket('1 hour', ts), src_call, receiver
WITH NO DATA;

CREATE MATERIALIZED VIEW ranking_statistics_1d
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	receiver,

	FIRST(first_position, ts) AS first_position,
	LAST(last_position, ts) AS last_position,
	MAX(normalized_quality) AS normalized_quality,
	MAX(distance) AS distance,
	MIN(altitude_min) AS altitude_min,
	MAX(altitude_max) AS altitude_max,

	SUM(points_motion) AS points_motion,
	SUM(points_static) AS points_static,
	SUM(points_fake) AS points_fake,
	SUM(points_error) AS points_error,
	SUM(points_total) AS points_total

FROM ranking_statistics_1h AS rs1h
GROUP BY time_bucket('1 day', ts), src_call, receiver
WITH NO DATA;

-- direction statistics 1d
CREATE MATERIALIZED VIEW direction_statistics_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	receiver,
	radial,
	relative_bearing,

	FIRST(first_position, ts) AS first_position,
	LAST(last_position, ts) AS last_position,
	MAX(normalized_quality) AS normalized_quality,
	MAX(distance) AS distance,
	MIN(altitude_min) AS altitude_min,
	MAX(altitude_max) AS altitude_max,

	SUM(points_motion) AS points_motion,
	SUM(points_static) AS points_static,
	SUM(points_fake) AS points_fake,
	SUM(points_error) AS points_error,
	SUM(points_total) AS points_total

FROM positions_5m AS p5m
GROUP BY time_bucket('1 day', ts), src_call, receiver, radial, relative_bearing
WITH NO DATA;
