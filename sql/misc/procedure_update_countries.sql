-- create a procedure which updates the countries of the receivers
DROP PROCEDURE IF EXISTS proc_update_countries();
CREATE PROCEDURE proc_update_countries()
LANGUAGE SQL
AS $$

UPDATE receivers AS r
SET iso2 = c.iso_a2_eh
FROM countries AS c
WHERE ST_Contains(c.geom, r.location);

$$;
