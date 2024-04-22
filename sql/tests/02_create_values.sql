SELECT
	'('
		|| 'TIMESTAMPTZ''' || ts || ''''
		|| ',' || '''' || src_call || ''''
		|| ',' || '''' || dst_call || ''''
		|| ',' || '''' || receiver || ''''
	    || ',' || '''' || receiver_time ||''''
		|| ',' || '''' || symbol_table || ''''
		|| ',' || '''' || CASE WHEN symbol_code = '''' THEN '''''' ELSE symbol_code END || ''''
		|| ',' || CASE WHEN course IS NULL THEN 'NULL::SMALLINT' ELSE course::TEXT END
		|| ',' || CASE WHEN speed IS NULL THEN 'NULL::SMALLINT' ELSE speed::TEXT END
		|| ',' || CASE WHEN altitude IS NULL THEN 'NULL::INTEGER' ELSE altitude::TEXT END
        || ',' || CASE WHEN aircraft_type IS NULL THEN 'NULL::SMALLINT' ELSE aircraft_type::TEXT END
		|| ',' || CASE WHEN climb_rate IS NULL THEN 'NULL::INTEGER' ELSE climb_rate::TEXT END
		|| ',' || CASE WHEN turn_rate IS NULL THEN 'NULL::DOUBLE PRECISION' ELSE turn_rate::TEXT END
		|| ',' || CASE WHEN receiver_ts IS NULL THEN 'NULL::TIMESTAMPTZ' ELSE 'TIMESTAMPTZ''' || receiver_ts || '''' END
		|| ',POINT(' || ST_X(location)::TEXT || ',' || ST_Y(location)::TEXT || ')::GEOMETRY'
	|| ')' AS total,
	*
FROM positions
WHERE receiver = 'Koenigsdf' AND ts BETWEEN '2024-03-29 10:00:00' AND '2024-03-29 11:00:00'
ORDER BY src_call, ts