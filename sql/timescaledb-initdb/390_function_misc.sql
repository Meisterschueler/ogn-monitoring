CREATE OR REPLACE FUNCTION iso2_to_emoji(iso2 TEXT)
RETURNS TEXT
AS $$

DECLARE
  flag_emoji TEXT;
BEGIN
  SELECT INTO flag_emoji
    CHR(ASCII(SUBSTRING(iso2, 1, 1)) + CAST(x'1f1a5' AS INT))
    || CHR(ASCII(SUBSTRING(iso2, 2, 1)) + CAST(x'1f1a5' AS INT));
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
