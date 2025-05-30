-- create a function which updates receiver confirmations and returns the proceeded rows
CREATE OR REPLACE FUNCTION update_records(lower TIMESTAMPTZ, upper TIMESTAMPTZ)
RETURNS INTEGER AS $$
DECLARE
	processed_position_rows INTEGER;
BEGIN

EXECUTE '
    WITH records AS (
        SELECT
            time_bucket(''1 day'', ts) AS ts,
            src_call,
            receiver,

            ts_first,
            ts_last,
            distance_min,
            distance_max,
            altitude_min,
            altitude_max,
            normalized_quality_min,
            normalized_quality_max
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY ts::DATE, src_call, receiver ORDER BY distance_max DESC) AS row
            FROM positions_5m
            WHERE
                ts BETWEEN TIMESTAMP''' || lower || ''' AND TIMESTAMP''' || upper || '''
                AND plausibility = 0
                AND distance_max IS NOT NULL
                AND altitude_max IS NOT NULL
                AND normalized_quality_max IS NOT NULL

				AND dst_call IN (''OGFLR'', ''OGNFNT'', ''OGNTRK'')
        ) AS sq
        WHERE sq.row = 1
    )

    INSERT INTO records_1d AS r (ts, src_call, receiver, ts_first, ts_last, distance_max, distance_min, altitude_max, altitude_min, normalized_quality_max, normalized_quality_min)
    SELECT * FROM records
    ON CONFLICT (ts, src_call, receiver) DO UPDATE
    SET
        ts_first = CASE WHEN r.distance_max < excluded.distance_max THEN excluded.ts_first ELSE r.ts_first END,
        ts_last = CASE WHEN r.distance_max < excluded.distance_max THEN excluded.ts_last ELSE r.ts_last END,
        distance_max = CASE WHEN r.distance_max < excluded.distance_max THEN excluded.distance_max ELSE r.distance_max END,
        distance_min = CASE WHEN r.distance_max < excluded.distance_max THEN excluded.distance_min ELSE r.distance_min END,
        altitude_max = CASE WHEN r.distance_max < excluded.distance_max THEN excluded.altitude_max ELSE r.altitude_max END,
        altitude_min = CASE WHEN r.distance_max < excluded.distance_max THEN excluded.altitude_min ELSE r.altitude_min END,
        normalized_quality_max = CASE WHEN r.distance_max < excluded.distance_max THEN excluded.normalized_quality_max ELSE r.normalized_quality_max END,
        normalized_quality_min = CASE WHEN r.distance_max < excluded.distance_max THEN excluded.normalized_quality_min ELSE r.normalized_quality_min END;
';

	GET DIAGNOSTICS processed_position_rows = ROW_COUNT;
	RETURN processed_position_rows;

END;
$$ LANGUAGE plpgsql;
