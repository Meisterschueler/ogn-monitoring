-- sender statistics
CREATE VIEW ranking_statistics_sender
AS
SELECT
	time_bucket('1 day', rs1h.ts) AS ts,
	src_call,

	SUM(rs1h.points_active) AS points_active,
	SUM(rs1h.points_good) AS points_good,
	SUM(rs1h.points_total) AS points_total,
	MAX(rs1h.normalized_quality) AS normalized_quality,
	MAX(rs1h.distance) AS distance,

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

	SUM(rs1h.points_active) AS points_active,
	SUM(rs1h.points_good) AS points_good,
	SUM(rs1h.points_total) AS points_total,
	MAX(rs1h.normalized_quality) AS normalized_quality,
	MAX(rs1h.distance) AS distance,

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
