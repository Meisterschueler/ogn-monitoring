-- join the ddb with registrations - because "IS SIMILAR TO regex" is quite expensive
-- a refresh should be done only if ddb or registration is changed
CREATE MATERIALIZED VIEW ddb_registration
AS
SELECT
	d.*,
	r.*,
	SUM(CASE WHEN registration != '' THEN 1 ELSE 0 END) OVER (PARTITION BY registration) AS double_registration,
	SUM(CASE WHEN registration != '' THEN 1 ELSE 0 END) OVER (PARTITION BY registration, address_type) AS double_registration_address_type,
	SUM(CASE WHEN registration != '' THEN 1 ELSE 0 END) OVER (PARTITION BY registration, cn, model) AS double_registration_cn_model
FROM ddb AS d
LEFT JOIN registrations AS r ON d.registration SIMILAR TO r.regex;

-- create sender view with ALL relevant informations
CREATE MATERIALIZED VIEW sender_ddb_openaip
AS
SELECT
	s.name AS sender_name,
	s.last_position AS sender_last_position,
	s.last_status AS sender_last_status,
	s.location AS sender_location,
	s.altitude AS sender_altitude,
	s.address_type AS sender_address_type,
	s.aircraft_type AS sender_aircraft_type,
	s.is_stealth AS sender_is_stealth,
	s.is_notrack AS sender_is_notrack,
	s.address AS sender_address,
	s.software_version AS sender_software_version,
	s.hardware_version AS sender_hardware_version,
	s.original_address AS sender_original_address,
	dr.address AS ddb_address,
	dr.address_type AS ddb_address_type,
	dr.model AS ddb_model,
	dr.model_type AS ddb_model_type,
	dr.registration AS ddb_registration,
	dr.cn AS ddb_cn,
	dr.is_notrack AS ddb_is_notrack,
	dr.is_noident AS ddb_is_noident,
	dr.iso2 AS registration_iso2,
	dr.regex AS registration_regex,
	dr.description AS registration_description,
	dr.aircraft_types AS registration_aircraft_types,
	fh.manufacturer AS flarm_hardware_manufacturer,
	fh.model AS flarm_hardware_model,
	fe.expiry_date AS flarm_expiry_date,
	i.iso2 AS icao24bit_iso2,
	i.lower_limit AS icao24bit_lower_limit,
	i.upper_limit AS icao24bit_upper_limit,
	a.name AS airport_name,
	a.code AS airport_code,
	a.iso2 AS airport_iso2,
	a.location AS airport_location,
	a.altitude AS airport_altitude,
	a.style AS airport_style,
	CASE 
        WHEN s.aircraft_type IS NULL OR dr.aircraft_types IS NULL THEN ''
        WHEN s.aircraft_type = ANY(dr.aircraft_types) THEN 'OK'
        WHEN 0 = ALL(dr.aircraft_types) THEN 'GENERIC'
        ELSE 'ERROR'
    END AS check_registration_aircraft_types,
	CASE
		WHEN double_registration IS NULL THEN ''
		WHEN double_registration = 0 THEN ''
		WHEN double_registration = 1 THEN 'OK'
		WHEN double_registration = double_registration_cn_model AND double_registration_address_type = 1 THEN 'WARNING'
		ELSE 'ERROR'
	END AS check_registration_double,
	CASE
		WHEN 
			(s.name LIKE 'ICA%' OR s.name LIKE 'PAW%')
			AND i.iso2 IS NOT NULL
			AND dr.iso2 IS NOT NULL
			AND i.iso2 != dr.iso2
		THEN 'ERROR'
		WHEN 
			(s.name LIKE 'ICA%' OR s.name LIKE 'PAW%')
			AND i.iso2 IS NOT NULL
			AND dr.iso2 IS NOT NULL
			AND i.iso2 = dr.iso2
		THEN 'OK'
		ELSE ''
	END AS check_iso2,
	CASE
		WHEN dr.model = '' OR dr.model IS NULL THEN ''
		WHEN s.aircraft_type = 1 AND dr.model_type = 1 THEN 'OK'			-- (moto-)glider -> Gliders/motoGliders
		WHEN s.aircraft_type = 2 AND dr.model_type IN (1,2,3) THEN 'OK'		-- tow plane -> Gliders/motoGliders, Planes or Ultralight
		WHEN s.aircraft_type = 3 AND dr.model_type = 4 THEN 'OK'			-- helicopter -> Helicopter
		WHEN s.aircraft_type = 6 AND dr.model_type = 6 AND dr.model = 'HangGlider' THEN 'OK'  -- hang-glider -> Others::HangGlider
		WHEN s.aircraft_type = 7 AND dr.model_type = 6 AND dr.model = 'Paraglider' THEN 'OK'  -- para-glider -> Others::Paraglider
		WHEN s.aircraft_type = 8 AND dr.model_type IN (2,3) THEN 'OK'		-- powered aircraft -> Planes or Ultralight
		WHEN s.aircraft_type = 9 AND dr.model_type = 2 THEN 'OK'			-- jet aircraft -> Planes
		WHEN s.aircraft_type = 10 AND dr.model_type = 6 AND dr.model = 'UFO' THEN 'OK'		-- UFO -> Others::UFO
		WHEN s.aircraft_type = 11 AND dr.model_type = 6 AND dr.model = 'Balloon' THEN 'OK'	-- Balloon -> Others::Balloon
		WHEN s.aircraft_type = 13 AND dr.model_type = 5 THEN 'OK'			-- UAV -> Drones/UAV
		WHEN s.aircraft_type = 14 AND dr.model_type = 6 AND dr.model = 'Ground Station' THEN 'OK'	-- ground support -> Others::Ground Station
		WHEN s.aircraft_type = 15 AND dr.model_type = 6 AND dr.model = 'Ground Station' THEN 'OK'	-- static object -> Others::Ground Station
		WHEN s.aircraft_type IS NULL THEN 'UNKNOWN'
		ELSE 'ERROR'
	END AS check_model_type,
	CASE
        WHEN s.address_type IS NULL OR dr.address_type IS NULL THEN 'UNKNOWN'
        WHEN s.address_type != dr.address_type THEN 'ERROR'
        ELSE 'OK'
    END AS check_address_type,
	CASE
		WHEN fe.expiry_date IS NULL THEN ''
		WHEN fe.expiry_date - NOW() > INTERVAL'1 year' THEN 'OK'
		WHEN fe.expiry_date - NOW() > INTERVAL'2 months' THEN 'WARNING'
		WHEN fe.expiry_date - NOW() > INTERVAL'1 day' THEN 'DANGER'
		ELSE 'EXPIRED'
	END AS check_flarm_expiry_date,
	CASE
		WHEN dr.registration IS NULL OR w.registration IS NULL THEN ''
		WHEN dr.registration IS NOT NULL AND w.registration IS NOT NULL and dr.registration = w.registration THEN 'OK'
		ELSE 'ERROR'
	END AS check_weglide_registration
FROM senders AS s
LEFT JOIN ddb_registration AS dr ON s.address = dr.address
LEFT JOIN flarm_hardware AS fh ON s.hardware_version = fh.id
LEFT JOIN flarm_expiry AS fe ON s.software_version = fe.version
LEFT JOIN icao24bit AS i ON
	s.address BETWEEN lower_limit AND upper_limit
	AND (s.name LIKE 'ICA%' OR s.name LIKE 'PAW%')
LEFT JOIN weglide AS w ON s.address = w.address
CROSS JOIN LATERAL (
	SELECT *
	FROM openaip
	ORDER BY openaip.location <-> s.location
	LIMIT 1
) AS a;
CREATE INDEX idx_sender_ddb_openaip_airport_iso2_airport_name ON sender_ddb_openaip (airport_iso2, airport_name);
