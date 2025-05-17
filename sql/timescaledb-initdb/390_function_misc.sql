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


CREATE OR REPLACE FUNCTION address_type_to_string(address_type INTEGER) RETURNS TEXT AS $$
DECLARE
  result TEXT := '';
BEGIN

  result := CASE address_type
    WHEN 1 THEN 'ICAO'
    WHEN 2 THEN 'FLARM'
    WHEN 3 THEN 'OGN'
    ELSE '?'
  END;

  RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION aircraft_type_to_string(sender_aircraft_type INTEGER) RETURNS TEXT AS $$
DECLARE
  result TEXT := '';
BEGIN

	result := CASE sender_aircraft_type
		WHEN 1 THEN '(MOTOR)GLIDER'
		WHEN 2 THEN 'TOWPLANE'
		WHEN 3 THEN 'HELICOPTER'
		WHEN 4 THEN 'SKYDIVER'
		WHEN 5 THEN 'DROPPLANE'
		WHEN 6 THEN 'HANGGLIDER'
		WHEN 7 THEN 'PARAGLIDER'
		WHEN 8 THEN 'PLANE'
		WHEN 9 THEN 'JET'
		WHEN 10 THEN 'UFO'
		WHEN 11 THEN 'BALLOON'
		WHEN 12 THEN 'AIRSHIP'
		WHEN 13 THEN 'UAV'
		WHEN 14 THEN 'GROUND SUPPORT'
		WHEN 15 THEN 'STATIC OBJECT'
		ELSE '???'
	END;

	RETURN result;
END;
$$ LANGUAGE plpgsql;

