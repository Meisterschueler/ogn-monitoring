-- create a procedure which updates the daily receiver rankings
DROP PROCEDURE IF EXISTS proc_update_rankings();
CREATE PROCEDURE proc_update_rankings()
LANGUAGE SQL
AS $$

SELECT
	sq.*,
	ROW_NUMBER() OVER (PARTITION BY sq.ts, sq.iso2 ORDER BY sq.distance DESC) AS distance_rank_local,
	ROW_NUMBER() OVER (PARTITION BY sq.ts ORDER BY sq.distance DESC) AS distance_rank_global,
	ROW_NUMBER() OVER (PARTITION BY sq.ts, sq.iso2 ORDER BY sq.points DESC) AS points_rank_local,
	ROW_NUMBER() OVER (PARTITION BY sq.ts ORDER BY sq.points DESC) AS points_rank_global,
	ROW_NUMBER() OVER (PARTITION BY sq.ts, sq.iso2 ORDER BY sq.sender_count DESC) AS sender_count_rank_local,
	ROW_NUMBER() OVER (PARTITION BY sq.ts ORDER BY sq.sender_count DESC) AS sender_count_rank_global,
	COUNT(*) OVER (PARTITION BY sq.ts, sq.iso2) AS count_local,
	COUNT(*) OVER (PARTITION BY sq.ts) AS count_global
FROM (
	SELECT
		rs.ts::date AS ts,
		rs.receiver,
		r.iso2,

		SUM(rs.points_total) AS points,
		MAX(rs.distance) AS distance,
		MAX(rs.normalized_quality) AS normalized_quality,
	
		COUNT(DISTINCT rs.src_call) AS sender_count
	FROM
		ranking_statistics_1d AS rs
	INNER JOIN receivers AS r ON rs.receiver = r.name
	WHERE rs.distance IS NOT NULL
	GROUP BY rs.ts::date, receiver, r.iso2
) AS sq


$$;
