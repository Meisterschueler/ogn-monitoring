CREATE OR REPLACE FUNCTION update_elevations(start_time TIMESTAMP, end_time TIMESTAMP, step INTERVAL)
  RETURNS void
AS $$
DECLARE
  ts_from TIMESTAMP;
  ts_to TIMESTAMP;
  ts_started TIMESTAMP;
BEGIN
  ts_from := start_time;
  
  WHILE ts_from < end_time LOOP
  	ts_to := ts_from + step;
	ts_started := clock_timestamp();

	EXECUTE '
		UPDATE positions AS p
			SET elevation = ST_Value(e.rast, p.location)
		FROM elevations AS e
		WHERE
			p.ts BETWEEN ''' || ts_from || ''' AND ''' || ts_to || '''
			AND p.location IS NOT NULL
			AND p.elevation IS NULL
			AND e.rast ~ p.location;
	';
	
	RAISE WARNING 'update_elevations (% - %s) executed in: %s', ts_from, ts_to, clock_timestamp() - ts_started;
	ts_from := ts_to;
  END LOOP;
END;
$$
LANGUAGE plpgsql;
