CREATE OR REPLACE FUNCTION iso2_to_emoji(iso2 TEXT)
RETURNS TEXT
AS $$

DECLARE
  flag_emoji TEXT;
BEGIN
  SELECT INTO flag_emoji
    CASE
		WHEN iso2 != '' THEN CHR(ASCII(SUBSTRING(iso2, 1, 1)) + CAST(x'1f1a5' AS INT))
    		|| CHR(ASCII(SUBSTRING(iso2, 2, 1)) + CAST(x'1f1a5' AS INT))
		ELSE ''
	END;
  RETURN flag_emoji;
END;

$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION icao24bit(address INTEGER)
RETURNS TEXT
AS $$

DECLARE
  icao TEXT;
BEGIN
  SELECT INTO icao
    LPAD(UPPER(to_hex(address)), 6, '0');
  RETURN icao;
END;

$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION ST_CardinalDirection(azimuth float8)
RETURNS CHARACTER VARYING
AS $$

SELECT
	CASE
		WHEN azimuth < 11.25 THEN 'N'
		WHEN azimuth < 33.75 THEN 'NNE'
		WHEN azimuth < 56.25 THEN 'NE'
		WHEN azimuth < 78.75 THEN 'ENE'
		WHEN azimuth < 101.25 THEN 'E'
		WHEN azimuth < 123.75 THEN 'ESE'
		WHEN azimuth < 146.25 THEN 'SE'
		WHEN azimuth < 168.75 THEN 'SSE'
		WHEN azimuth < 191.25 THEN 'S'
		WHEN azimuth < 213.75 THEN 'SSW'
		WHEN azimuth < 236.25 THEN 'SW'
		WHEN azimuth < 258.75 THEN 'WSW'
		WHEN azimuth < 281.25 THEN 'W'
		WHEN azimuth < 303.75 THEN 'WNW'
		WHEN azimuth < 326.25 THEN 'NW'
		WHEN azimuth < 348.75 THEN 'NNW'
		ELSE 'N'
	END;

$$ LANGUAGE sql;



CREATE OR REPLACE FUNCTION update_countries()
RETURNS INTEGER
AS $$

DECLARE
  processed_rows INTEGER;
BEGIN
	WITH rows AS (
	  UPDATE receivers AS r
		SET iso2 = c.iso_a2_eh
		FROM countries AS c
		WHERE ST_Contains(c.geom, r.location)
		RETURNING 1
	)
	
	SELECT INTO processed_rows
		COUNT(*)
	FROM rows;
	RETURN processed_rows;
END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION hex_to_string(hex_string text) RETURNS text AS $$
DECLARE
  result text := '';
BEGIN
	FOR i IN 1..LENGTH(hex_string) BY 2 LOOP
	  result := result || CHR(('x' || SUBSTRING(hex_string FROM i FOR 2))::bit(8)::int);
	END LOOP;
  
  RETURN result;
END;
$$ LANGUAGE plpgsql;
