UPDATE openaip
	SET tzid = tz.tzid
FROM timezones AS tz
WHERE ST_Intersects(location, tz.geom);
