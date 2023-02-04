-- create table with current ranges
DROP TABLE IF EXISTS update_range CASCADE;
CREATE TEMP TABLE update_range AS
SELECT
	COALESCE(MAX(r.last_seen), TIMESTAMP '1970-01-01 00:00:00+00') AS receiver_start,
	COALESCE(MAX(s.last_seen), TIMESTAMP '1970-01-01 00:00:00+00') AS sender_start
FROM receivers AS r, senders AS s;

-- create view with recently receiver messages
DROP VIEW IF EXISTS receiver_messages;
CREATE TEMP VIEW receiver_messages AS
SELECT p.*
FROM positions AS p, update_range AS ur
WHERE
    p.dst_call = 'OGNSDR'
    AND p.ts > ur.receiver_start
ORDER BY ts
LIMIT 1000000;

-- create/update receivers - first and last seen
INSERT INTO receivers AS r (name, first_seen, last_seen)
SELECT
	rm.src_call AS name,
	MIN(rm.ts) AS first_seen,
	MAX(rm.ts) AS last_seen
FROM receiver_messages as rm
GROUP BY rm.src_call
ON CONFLICT (name) DO UPDATE
SET last_seen = EXCLUDED.last_seen;

-- create view with recently inserted sender messages
DROP VIEW IF EXISTS sender_messages;
CREATE TEMP VIEW sender_messages AS
SELECT p.*
FROM positions AS p, update_range AS ur
WHERE
    p.dst_call = 'OGFLR'
    AND p.ts > ur.sender_start
ORDER BY ts
LIMIT 1000000;

-- create/update senders - first and last seen
INSERT INTO senders AS s (name, first_seen, last_seen)
SELECT
	sm.src_call AS name,
	MIN(sm.ts) AS first_seen,
	MAX(sm.ts) AS last_seen
FROM sender_messages AS sm
GROUP BY sm.src_call
ON CONFLICT (name) DO UPDATE
SET last_seen = EXCLUDED.last_seen;

-- create/update senders - software and hardware version
WITH ranked_sender_messages AS (
	SELECT sm.*, ROW_NUMBER() OVER (PARTITION BY src_call ORDER BY sm.ts DESC) AS rn
	FROM sender_messages AS sm
	WHERE
		sm.software_version IS NOT NULL
		AND sm.hardware_version IS NOT NULL
)

UPDATE senders
SET
	software_version = sq.software_version,
	hardware_version = sq.hardware_version
FROM (
	SELECT *
	FROM ranked_sender_messages AS rsm
	WHERE rsm.rn = 1
) AS sq
WHERE senders.name = sq.src_call;

-- create/update receivers - first and last seen, last receiption
INSERT INTO receivers AS r (name, first_seen, last_seen, last_receiption)
SELECT
	sm.receiver AS name,
	MIN(sm.ts) AS first_seen,
	MAX(sm.ts) AS last_seen,
    MAX(sm.ts) AS last_receiption
FROM sender_messages AS sm
GROUP BY sm.receiver
ON CONFLICT (name) DO UPDATE
SET
    last_seen = EXCLUDED.last_seen,
    last_receiption = EXCLUDED.last_receiption;

-- create/update base (5m) statistics - all messages
INSERT INTO statistics AS s (ts, sender, receiver, points_good, points_total)
SELECT
	date_trunc('hour', ts) + date_part('minute', ts)::int / 5 * interval '5 min' AS ts,
	src_call AS sender,
	receiver AS receiver,
    0 AS points_good,
	COUNT(*) AS points_total
FROM sender_messages AS sm
GROUP BY date_trunc('hour', ts) + date_part('minute', ts)::int / 5 * interval '5 min', src_call, receiver
ON CONFLICT (ts, sender, receiver) DO UPDATE
SET
    points_total = s.points_total + EXCLUDED.points_total;

-- create/update base (5m) statistics - only good messages
INSERT INTO statistics AS s (ts, sender, receiver, points_good, points_total, distance, normalized_quality)
SELECT
	date_trunc('hour', ts) + date_part('minute', ts)::int / 5 * interval '5 min' AS ts,
	src_call AS sender,
	receiver AS receiver,
	COUNT(*) AS points_good,
    0 AS points_total,
	MAX(distance) AS distance,
	MAX(normalized_quality) AS normalized_quality
FROM sender_messages AS sm
WHERE error IS NULL OR error = 0
GROUP BY date_trunc('hour', ts) + date_part('minute', ts)::int / 5 * interval '5 min', src_call, receiver
ON CONFLICT (ts, sender, receiver) DO UPDATE
SET
    points_good = s.points_good + EXCLUDED.points_good,
	distance = CASE
		WHEN EXCLUDED.distance IS NOT NULL AND (s.distance IS NULL OR s.distance < EXCLUDED.distance)
		THEN EXCLUDED.distance
		ELSE s.distance
	END,
	normalized_quality = CASE
		WHEN EXCLUDED.normalized_quality IS NOT NULL AND (s.normalized_quality IS NULL OR s.normalized_quality < EXCLUDED.normalized_quality)
		THEN EXCLUDED.normalized_quality
		ELSE s.normalized_quality
	END;

-- create/update sender statistics
INSERT INTO sender_statistics AS ss (ts, sender, points_good, points_total, distance, normalized_quality, receiver_count)
SELECT
	ts AS ts,
	sender AS sender,
	SUM(points_good) AS points_good,
	SUM(points_total) AS points_total,
	MAX(distance) AS distance,
	MAX(normalized_quality) AS normalized_quality,
	COUNT(*) AS receiver_count
FROM statistics AS s,
(
	SELECT COALESCE(MIN(date_trunc('hour', ts)), '1970-01-01 00:00:00+00') AS changed
	FROM sender_messages AS sm
) AS sq
WHERE s.ts >= sq.changed
GROUP BY ts, sender
ON CONFLICT (ts, sender) DO UPDATE
SET
	points_good = EXCLUDED.points_good,
	points_total = EXCLUDED.points_total,
	distance = EXCLUDED.distance,
	normalized_quality = EXCLUDED.normalized_quality,
	receiver_count = EXCLUDED.receiver_count;

-- create/update receiver statistics
INSERT INTO receiver_statistics AS rs (ts, receiver, points_good, points_total, distance, normalized_quality, sender_count)
SELECT
	ts AS ts,
	receiver AS receiver,
	SUM(points_good) AS points_good,
	SUM(points_total) AS points_total,
	MAX(distance) AS distance,
	MAX(normalized_quality) AS normalized_quality,
	COUNT(*) AS sender_count
FROM statistics AS s,
(
	SELECT COALESCE(MIN(date_trunc('hour', ts)), '1970-01-01 00:00:00+00') AS changed
	FROM receiver_messages AS rm
) AS sq
WHERE s.ts >= sq.changed
GROUP BY ts, receiver
ON CONFLICT (ts, receiver) DO UPDATE
SET
	points_good = EXCLUDED.points_good,
	points_total = EXCLUDED.points_total,
	distance = EXCLUDED.distance,
	normalized_quality = EXCLUDED.normalized_quality,
	sender_count = EXCLUDED.sender_count;