-- sender statistics
CREATE VIEW ranking_statistics_sender
AS
SELECT
	time_bucket('1 day', rs1h.ts) AS ts,
	src_call,

	FIRST(first_position, rs1h.ts) AS first_position,
	LAST(last_position, rs1h.ts) AS last_position,
	MAX(normalized_quality) AS normalized_quality,
	MAX(distance) AS distance,
	MIN(altitude_min) AS altitude_min,
	MAX(altitude_max) AS altitude_max,

	SUM(points_dynamic) AS points_dynamic,
	SUM(points_motion) AS points_motion,
	SUM(points_static) AS points_static,
	SUM(points_fake) AS points_fake,
	SUM(points_total) AS points_total,

	AVG(rs1h.normalized_quality - sq.avg_quality) AS relative_quality,

	COUNT(DISTINCT rs1h.receiver) AS receiver_count
FROM ranking_statistics_1h AS rs1h, (
	SELECT
		time_bucket('1 day', ts) AS ts,
		receiver,

		AVG(normalized_quality) AS avg_quality
	FROM ranking_statistics_1h
	GROUP BY time_bucket('1 day', ts), receiver
) AS sq
WHERE sq.ts = rs1h.ts AND sq.receiver = rs1h.receiver
GROUP BY time_bucket('1 day', rs1h.ts), rs1h.src_call;

-- sender statistics
CREATE VIEW ranking_statistics_receiver
AS
SELECT
	time_bucket('1 day', rs1h.ts) AS ts,
	receiver,

	FIRST(first_position, rs1h.ts) AS first_position,
	LAST(last_position, rs1h.ts) AS last_position,
	MAX(normalized_quality) AS normalized_quality,
	MAX(distance) AS distance,
	MIN(altitude_min) AS altitude_min,
	MAX(altitude_max) AS altitude_max,

	SUM(points_dynamic) AS points_dynamic,
	SUM(points_motion) AS points_motion,
	SUM(points_static) AS points_static,
	SUM(points_fake) AS points_fake,
	SUM(points_total) AS points_total,

	AVG(rs1h.normalized_quality - sq.avg_quality) AS relative_quality,

	COUNT(DISTINCT rs1h.src_call) AS sender_count
FROM ranking_statistics_1h AS rs1h, (
	SELECT
		time_bucket('1 day', ts) AS ts,
		src_call,

		AVG(normalized_quality) AS avg_quality
	FROM ranking_statistics_1h
	GROUP BY time_bucket('1 day', ts), src_call
) AS sq
WHERE sq.ts = rs1h.ts AND sq.src_call = rs1h.src_call
GROUP BY time_bucket('1 day', rs1h.ts), rs1h.receiver;
