-- 5m statistics
CREATE MATERIALIZED VIEW statistics_5m
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('5 minutes', ts) AS ts,
	src_call,
	receiver,
	CASE (error IS NOT NULL OR error = 0) AND bearing IS NOT NULL AND course IS NOT NULL AND speed >= 5
		WHEN FALSE THEN NULL
		ELSE CAST(((CAST(bearing AS INTEGER) + 15 + 180) % 360) / 30 AS INTEGER) * 30
	END AS radial,
	CASE (error IS NOT NULL OR error = 0) AND bearing IS NOT NULL AND course IS NOT NULL AND speed >= 5
		WHEN FALSE THEN NULL
		ELSE CAST(((CAST(bearing AS INTEGER) + 15 - course + 360) % 360) / 30 AS INTEGER) * 30
	END AS relative_bearing,

	COUNT(*) FILTER (WHERE (error IS NULL OR error <= 5) AND bearing IS NOT NULL AND course IS NOT NULL AND speed >= 5) AS points_active,
	COUNT(*) FILTER (WHERE error IS NULL or error <= 5) AS points_good,
	COUNT(*) AS points_total,
	MAX(normalized_quality) FILTER (WHERE (error IS NULL OR error <= 5) AND bearing IS NOT NULL AND course IS NOT NULL AND speed >= 5) AS normalized_quality,
	MAX(distance) FILTER (WHERE (error IS NULL OR error <= 5) AND bearing IS NOT NULL AND course IS NOT NULL AND speed >= 5) AS distance
FROM
	positions
WHERE
	src_call NOT LIKE 'RND%'
	AND dst_call IN ('APRS', 'OGFLR', 'OGNFNT', 'OGNTRK')
	AND receiver NOT LIKE 'GLIDERN%'
GROUP BY time_bucket('5 minutes', ts), src_call, receiver, radial, relative_bearing
WITH NO DATA;

-- ranking statistics 1h
CREATE MATERIALIZED VIEW ranking_statistics_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('1 hour', ts) AS ts,
	src_call,
	receiver,

	SUM(points_active) AS points_active,
	SUM(points_good) AS points_good,
	SUM(points_total) AS points_total,
	MAX(normalized_quality) AS normalized_quality,
	MAX(distance) AS distance

FROM statistics_5m AS s5m
GROUP BY time_bucket('1 hour', ts), src_call, receiver
WITH NO DATA;

CREATE MATERIALIZED VIEW ranking_statistics_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
        time_bucket('1 day', ts) AS ts,
        src_call,
        receiver,

        SUM(points_active) AS points_active,
        SUM(points_good) AS points_good,
        SUM(points_total) AS points_total,
        MAX(normalized_quality) AS normalized_quality,
        MAX(distance) AS distance

FROM ranking_statistics_1h AS rs1h
GROUP BY time_bucket('1 day', ts), src_call, receiver
WITH NO DATA;

-- quality statistics 1h
CREATE MATERIALIZED VIEW quality_statistics_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('1 hour', ts) AS ts,
	src_call,
	receiver,
	radial,
	relative_bearing,

	SUM(points_active) AS points_active,
	SUM(points_good) AS points_good,
	SUM(points_total) AS points_total,
	MAX(normalized_quality) AS normalized_quality,
	MAX(distance) AS distance

FROM statistics_5m AS s5m
GROUP BY time_bucket('1 hour', ts), src_call, receiver, radial, relative_bearing
WITH NO DATA;

CREATE MATERIALIZED VIEW quality_statistics_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
        time_bucket('1 day', ts) AS ts,
        src_call,
        receiver,
        radial,
        relative_bearing,

        SUM(points_active) AS points_active,
        SUM(points_good) AS points_good,
        SUM(points_total) AS points_total,
        MAX(normalized_quality) AS normalized_quality,
        MAX(distance) AS distance

FROM quality_statistics_1h AS qs1h
GROUP BY time_bucket('1 day', ts), src_call, receiver, radial, relative_bearing
WITH NO DATA;
