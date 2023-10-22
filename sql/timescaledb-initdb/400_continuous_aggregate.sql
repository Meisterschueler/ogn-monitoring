-- sender position statistics - base for position based sender statistics (records, directions, states)
CREATE MATERIALIZED VIEW sender_positions_5m
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('5 minutes', ts) AS ts,
	src_call,
	dst_call,
	receiver,
	plausibility,
	CAST(((CAST(bearing AS INTEGER) + 15 + 180) % 360) / 30 AS INTEGER) * 30 AS radial,
	CAST(((CAST(bearing AS INTEGER) + 15 - course + 360) % 360) / 30 AS INTEGER) * 30 AS relative_bearing,
	original_address,

	FIRST(ts, ts) AS ts_first,
	LAST(ts, ts) AS ts_last,
	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,

	LAST(address_type, ts) FILTER (WHERE address_type IS NOT NULL) AS address_type,
	LAST(aircraft_type, ts) FILTER (WHERE aircraft_type IS NOT NULL) AS aircraft_type,
	LAST(is_stealth, ts) FILTER (WHERE is_stealth IS NOT NULL) AS is_stealth,
	LAST(is_notrack, ts) FILTER (WHERE is_notrack IS NOT NULL) AS is_notrack,
	LAST(address, ts) FILTER (WHERE address IS NOT NULL) AS address,
	LAST(software_version, ts) FILTER (WHERE software_version IS NOT NULL) AS software_version,
	LAST(hardware_version, ts) FILTER (WHERE hardware_version IS NOT NULL) AS hardware_version,

	-- change indicator
	-- bit 0: address_type
	-- bit 1: aircraft_type
	-- bit 2: is_stealth
	-- bit 3: is_notrack
	-- bit 4: address
	-- bit 5: software_version
	-- bit 6: hardware_version
	0
	+ CASE WHEN MIN(address_type) FILTER (WHERE address_type IS NOT NULL) != MAX(address_type) FILTER (WHERE address_type IS NOT NULL) THEN 1 ELSE 0 END
	+ CASE WHEN MIN(aircraft_type) FILTER (WHERE aircraft_type IS NOT NULL) != MAX(aircraft_type) FILTER (WHERE aircraft_type IS NOT NULL) THEN 2 ELSE 0 END
	+ CASE WHEN MIN(CAST(is_stealth AS INTEGER)) FILTER (WHERE is_stealth IS NOT NULL) != MAX(CAST(is_stealth AS INTEGER)) FILTER (WHERE is_stealth IS NOT NULL) THEN 4 ELSE 0 END
	+ CASE WHEN MIN(CAST(is_notrack AS INTEGER)) FILTER (WHERE is_notrack IS NOT NULL) != MAX(CAST(is_notrack AS INTEGER)) FILTER (WHERE is_notrack IS NOT NULL) THEN 8 ELSE 0 END
	+ CASE WHEN MIN(address) FILTER (WHERE address IS NOT NULL) != MAX(address) FILTER (WHERE address IS NOT NULL) THEN 16 ELSE 0 END
	+ CASE WHEN MIN(software_version) FILTER (WHERE software_version IS NOT NULL) != MAX(software_version) FILTER (WHERE software_version IS NOT NULL) THEN 32 ELSE 0 END
	+ CASE WHEN MIN(hardware_version) FILTER (WHERE hardware_version IS NOT NULL) != MAX(hardware_version) FILTER (WHERE hardware_version IS NOT NULL) THEN 64 ELSE 0 END
	AS changed,

	MIN(distance) AS distance_min,
	MAX(distance) AS distance_max,
	MIN(altitude) AS altitude_min,
	MAX(altitude) AS altitude_max,
	MIN(normalized_quality) AS normalized_quality_min,
	MAX(normalized_quality) AS normalized_quality_max,

	COUNT(*) AS messages
FROM positions
WHERE
	src_call NOT LIKE 'RND%'
	AND receiver NOT LIKE 'GLIDERN%'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
WITH NO DATA;

-- direction statistics (sender -> receiver) for polar diagram
CREATE MATERIALIZED VIEW directions_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('15 minutes', ts) AS ts,
	src_call,
	receiver,
	radial,
	relative_bearing,
	
	MAX(distance_max) AS distance,
	MAX(normalized_quality_max) AS normalized_quality,
	
	SUM(messages) AS messages,
	COUNT(*) AS buckets_5m
FROM sender_positions_5m
WHERE
	radial IS NOT NULL AND relative_bearing IS NOT NULL AND distance_max IS NOT NULL and normalized_quality_max IS NOT NULL	
	AND plausibility IS NOT NULL AND plausibility != -1
	AND plausibility & b'11110000000000'::INTEGER = 0	-- no fake signal_quality, no fake distance
	
	--AND COALESCE(speed, 0) >= 5 AND ABS(COALESCE(climb_rate, 0)) < 2000 AND ABS(COALESCE(turn_rate, 0)) * speed < 30
GROUP BY 1, 2, 3, 4, 5
WITH NO DATA;

CREATE MATERIALIZED VIEW receiver_directions_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	receiver,
	radial,
	relative_bearing,
	
	MAX(distance) AS distance,
	MAX(normalized_quality) AS normalized_quality,
	
	SUM(messages) AS messages,
	SUM(buckets_5m) AS buckets_5m,
	COUNT(*) AS buckets_15m
FROM directions_15m
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE MATERIALIZED VIEW sender_directions_1d
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	radial,
	relative_bearing,
	
	MAX(distance) AS distance,
	MAX(normalized_quality) AS normalized_quality,
	
	SUM(messages) AS messages,
	SUM(buckets_5m) AS buckets_5m,
	COUNT(*) AS buckets_15m
FROM directions_15m
GROUP BY 1, 2, 3, 4
WITH NO DATA;

-- sender position states
CREATE MATERIALIZED VIEW sender_position_states_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('15 minutes', ts) AS ts,
	src_call,
	original_address,
	
	FIRST(ts_first, ts) AS ts_first,
	LAST(ts_last, ts) AS ts_last,
	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,
	
	LAST(address_type, ts) FILTER (WHERE address_type IS NOT NULL) AS address_type,
	LAST(aircraft_type, ts) FILTER (WHERE aircraft_type IS NOT NULL) AS aircraft_type,
	LAST(is_stealth, ts) FILTER (WHERE is_stealth IS NOT NULL) AS is_stealth,
	LAST(is_notrack, ts) FILTER (WHERE is_notrack IS NOT NULL) AS is_notrack,
	LAST(address, ts) FILTER (WHERE address IS NOT NULL) AS address,
	LAST(software_version, ts) FILTER (WHERE software_version IS NOT NULL) AS software_version,
	LAST(hardware_version, ts) FILTER (WHERE hardware_version IS NOT NULL) AS hardware_version,
	
	BIT_OR(changed)
	| CASE WHEN MIN(address_type) FILTER (WHERE address_type IS NOT NULL) != MAX(address_type) FILTER (WHERE address_type IS NOT NULL) THEN 1 ELSE 0 END
	| CASE WHEN MIN(aircraft_type) FILTER (WHERE aircraft_type IS NOT NULL) != MAX(aircraft_type) FILTER (WHERE aircraft_type IS NOT NULL) THEN 2 ELSE 0 END
	| CASE WHEN MIN(CAST(is_stealth AS INTEGER)) FILTER (WHERE is_stealth IS NOT NULL) != MAX(CAST(is_stealth AS INTEGER)) FILTER (WHERE is_stealth IS NOT NULL) THEN 4 ELSE 0 END
	| CASE WHEN MIN(CAST(is_notrack AS INTEGER)) FILTER (WHERE is_notrack IS NOT NULL) != MAX(CAST(is_notrack AS INTEGER)) FILTER (WHERE is_notrack IS NOT NULL) THEN 8 ELSE 0 END
	| CASE WHEN MIN(address) FILTER (WHERE address IS NOT NULL) != MAX(address) FILTER (WHERE address IS NOT NULL) THEN 16 ELSE 0 END
	| CASE WHEN MIN(software_version) FILTER (WHERE software_version IS NOT NULL) != MAX(software_version) FILTER (WHERE software_version IS NOT NULL) THEN 32 ELSE 0 END
	| CASE WHEN MIN(hardware_version) FILTER (WHERE hardware_version IS NOT NULL) != MAX(hardware_version) FILTER (WHERE hardware_version IS NOT NULL) THEN 64 ELSE 0 END
	AS changed,

	SUM(messages) AS messages,
	COUNT(*) AS buckets_5m
FROM sender_positions_5m
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE MATERIALIZED VIEW sender_position_states_1d
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	original_address,
	
	FIRST(ts_first, ts) AS ts_first,
	LAST(ts_last, ts) AS ts_last,
	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,
	
	LAST(address_type, ts) FILTER (WHERE address_type IS NOT NULL) AS address_type,
	LAST(aircraft_type, ts) FILTER (WHERE aircraft_type IS NOT NULL) AS aircraft_type,
	LAST(is_stealth, ts) FILTER (WHERE is_stealth IS NOT NULL) AS is_stealth,
	LAST(is_notrack, ts) FILTER (WHERE is_notrack IS NOT NULL) AS is_notrack,
	LAST(address, ts) FILTER (WHERE address IS NOT NULL) AS address,
	LAST(software_version, ts) FILTER (WHERE software_version IS NOT NULL) AS software_version,
	LAST(hardware_version, ts) FILTER (WHERE hardware_version IS NOT NULL) AS hardware_version,
	
	BIT_OR(changed)
	| CASE WHEN MIN(address_type) FILTER (WHERE address_type IS NOT NULL) != MAX(address_type) FILTER (WHERE address_type IS NOT NULL) THEN 1 ELSE 0 END
	| CASE WHEN MIN(aircraft_type) FILTER (WHERE aircraft_type IS NOT NULL) != MAX(aircraft_type) FILTER (WHERE aircraft_type IS NOT NULL) THEN 2 ELSE 0 END
	| CASE WHEN MIN(CAST(is_stealth AS INTEGER)) FILTER (WHERE is_stealth IS NOT NULL) != MAX(CAST(is_stealth AS INTEGER)) FILTER (WHERE is_stealth IS NOT NULL) THEN 4 ELSE 0 END
	| CASE WHEN MIN(CAST(is_notrack AS INTEGER)) FILTER (WHERE is_notrack IS NOT NULL) != MAX(CAST(is_notrack AS INTEGER)) FILTER (WHERE is_notrack IS NOT NULL) THEN 8 ELSE 0 END
	| CASE WHEN MIN(address) FILTER (WHERE address IS NOT NULL) != MAX(address) FILTER (WHERE address IS NOT NULL) THEN 16 ELSE 0 END
	| CASE WHEN MIN(software_version) FILTER (WHERE software_version IS NOT NULL) != MAX(software_version) FILTER (WHERE software_version IS NOT NULL) THEN 32 ELSE 0 END
	| CASE WHEN MIN(hardware_version) FILTER (WHERE hardware_version IS NOT NULL) != MAX(hardware_version) FILTER (WHERE hardware_version IS NOT NULL) THEN 64 ELSE 0 END
	AS changed,

	SUM(messages) AS messages,
	SUM(buckets_5m) AS buckets_5m,
	COUNT(*) AS buckets_15m
FROM sender_position_states_15m
GROUP BY 1, 2, 3
WITH NO DATA;

-- receiver position statistics
CREATE MATERIALIZED VIEW receiver_position_states_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('15 minutes', ts) AS ts,
	src_call,
	dst_call,
	
	FIRST(ts, ts) AS ts_first,
	LAST(ts, ts) AS ts_last,
	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,

	-- changes
	-- bit 0: location
	-- bit 1: altitude
	0
	+ CASE WHEN MIN(location) != MAX(location) THEN 1 ELSE 0 END
	+ CASE WHEN MIN(altitude) != MAX(altitude) THEN 2 ELSE 0 END
	AS changed,

	COUNT(*) AS messages
FROM positions AS p
WHERE
	src_call NOT LIKE 'RND%'
	AND receiver LIKE 'GLIDERN%'
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE MATERIALIZED VIEW receiver_position_states_1d
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	
	FIRST(ts_first, ts) AS ts_first,
	LAST(ts_last, ts) AS ts_last,
	LAST(location, ts) AS location,
	LAST(altitude, ts) AS altitude,

	BIT_OR(changed)
	| CASE WHEN MIN(location) != MAX(location) THEN 1 ELSE 0 END
	| CASE WHEN MIN(altitude) != MAX(altitude) THEN 2 ELSE 0 END
	AS changed,

	SUM(messages) AS messages,
	COUNT(*) AS buckets_15m
FROM receiver_position_states_15m
GROUP BY 1, 2
WITH NO DATA;


-- receiver status states
CREATE MATERIALIZED VIEW receiver_status_states_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('15 minutes', ts) AS ts,
	src_call,
	
	FIRST(ts, ts) AS ts_first,
	LAST(ts, ts) AS ts_last,
	LAST(version, ts) FILTER (WHERE version IS NOT NULL) AS version,
	LAST(platform, ts) FILTER (WHERE platform IS NOT NULL) AS platform,

	-- changes
	-- bit 0: version changed
	-- bit 1: platform changed
	0
	+ CASE WHEN MIN(version) FILTER (WHERE version IS NOT NULL) != MAX(version) FILTER (WHERE version IS NOT NULL) THEN 1 ELSE 0 END
	+ CASE WHEN MIN(platform) FILTER (WHERE platform IS NOT NULL) != MAX(platform) FILTER (WHERE platform IS NOT NULL) THEN 2 ELSE 0 END
	AS changed,

	COUNT(*) AS messages
FROM statuses
WHERE
	src_call NOT LIKE 'RND%'
	AND receiver LIKE 'GLIDERN%'
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW receiver_status_states_1d
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,

	FIRST(ts_first, ts) AS ts_first,
	LAST(ts_last, ts) AS ts_last,
	LAST(version, ts) AS version,
	LAST(platform, ts) AS platform,

	BIT_OR(changed)
	| CASE WHEN MIN(version) FILTER (WHERE version IS NOT NULL) != MAX(version) FILTER (WHERE version IS NOT NULL) THEN 1 ELSE 0 END
	| CASE WHEN MIN(platform) FILTER (WHERE platform IS NOT NULL) != MAX(platform) FILTER (WHERE platform IS NOT NULL) THEN 2 ELSE 0 END
	AS changed,

	SUM(messages) AS messages,
	COUNT(*) AS buckets_15m
FROM receiver_status_states_15m
GROUP BY 1, 2
WITH NO DATA;


-- sender position statistics
CREATE MATERIALIZED VIEW sender_position_statistics_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('15 minutes', ts) AS ts,
	src_call,
	receiver,

	MIN(ts_first) AS ts_first,
	MAX(ts_last) AS ts_last,
	
	SUM(messages) AS messages,
	SUM(messages) FILTER (WHERE plausibility IS NULL) AS messages_invalid,
	SUM(messages) FILTER (WHERE plausibility IS NOT NULL AND plausibility = -1) AS messages_unplausible,
	SUM(messages) FILTER (WHERE plausibility IS NOT NULL AND plausibility != -1 AND plausibility & b'11110000000000'::INTEGER > 0) AS messages_fake,
	SUM(messages) FILTER (WHERE plausibility IS NOT NULL AND plausibility != -1 AND plausibility & b'11110000000000'::INTEGER = 0 AND plausibility & b'00000000111110'::INTEGER > 0) AS messages_bad,
	SUM(messages) FILTER (WHERE plausibility IS NOT NULL AND plausibility != -1 AND plausibility & b'11110000111110'::INTEGER = 0 AND plausibility & b'00001000000000'::INTEGER > 0 AND plausibility & b'00000111000000'::INTEGER > 0) AS messages_unconfirmed,
	SUM(messages) FILTER (WHERE plausibility IS NOT NULL AND plausibility != -1 AND plausibility & b'11110000111110'::INTEGER = 0 AND plausibility & b'00001000000000'::INTEGER = 0 AND plausibility & b'00000111000000'::INTEGER > 0) AS messages_confirmed_indirectly,
	SUM(messages) FILTER (WHERE plausibility IS NOT NULL AND plausibility != -1 AND plausibility & b'11110000111110'::INTEGER = 0 AND plausibility & b'00000111000000'::INTEGER = 0) AS messages_confirmed_directly,
	
	MAX(distance_max) AS distance_max,
	MAX(distance_max) FILTER (WHERE plausibility IS NULL) AS distance_max_invalid,
	MAX(distance_max) FILTER (WHERE plausibility IS NOT NULL AND plausibility = -1) AS distance_max_unplausible,
	MAX(distance_max) FILTER (WHERE plausibility IS NOT NULL AND plausibility != -1 AND plausibility & b'11110000000000'::INTEGER > 0) AS distance_max_fake,
	MAX(distance_max) FILTER (WHERE plausibility IS NOT NULL AND plausibility != -1 AND plausibility & b'11110000000000'::INTEGER = 0 AND plausibility & b'00000000111110'::INTEGER > 0) AS distance_max_bad,
	MAX(distance_max) FILTER (WHERE plausibility IS NOT NULL AND plausibility != -1 AND plausibility & b'11110000111110'::INTEGER = 0 AND plausibility & b'00001000000000'::INTEGER > 0 AND plausibility & b'00000111000000'::INTEGER > 0) AS distance_max_unconfirmed,
	MAX(distance_max) FILTER (WHERE plausibility IS NOT NULL AND plausibility != -1 AND plausibility & b'11110000111110'::INTEGER = 0 AND plausibility & b'00001000000000'::INTEGER = 0 AND plausibility & b'00000111000000'::INTEGER > 0) AS distance_max_confirmed_indirectly,
	MAX(distance_max) FILTER (WHERE plausibility IS NOT NULL AND plausibility != -1 AND plausibility & b'11110000111110'::INTEGER = 0 AND plausibility & b'00000111000000'::INTEGER = 0) AS distance_max_confirmed_directly,

	COUNT(*) AS buckets_5m
FROM sender_positions_5m
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE MATERIALIZED VIEW sender_position_statistics_1d
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	receiver,
	
	MIN(ts_first) AS ts_first,
	MAX(ts_last) AS ts_last,

	SUM(messages) AS messages,
	SUM(messages_invalid) AS messages_invalid,
	SUM(messages_unplausible) AS messages_unplausible,
	SUM(messages_fake) AS messages_fake,
	SUM(messages_bad) AS messages_bad,
	SUM(messages_unconfirmed) AS messages_unconfirmed,
	SUM(messages_confirmed_indirectly) AS messages_confirmed_indirectly,
	SUM(messages_confirmed_directly) AS messages_confirmed_directly,
	
	MAX(distance_max) AS distance_max,
	MAX(distance_max_invalid) AS distance_max_invalid,
	MAX(distance_max_unplausible) AS distance_max_unplausible,
	MAX(distance_max_fake) AS distance_max_fake,
	MAX(distance_max_bad) AS distance_max_bad,
	MAX(distance_max_unconfirmed) AS distance_max_unconfirmed,
	MAX(distance_max_confirmed_indirectly) AS distance_max_confirmed_indirectly,
	MAX(distance_max_confirmed_directly) AS distance_max_confirmed_directly,

	SUM(buckets_5m) AS buckets_5m,
	COUNT(*) AS buckets_15m
FROM sender_position_statistics_15m
GROUP BY 1, 2, 3
WITH NO DATA;

-- sender statistics
CREATE MATERIALIZED VIEW sender_statistics_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('15 min', ts) AS ts,
	src_call,
	
	COUNT(*) AS receivers,

	MIN(ts_first) AS ts_first,
	MAX(ts_last) AS ts_last,
	
	SUM(messages) AS messages,
	SUM(messages_invalid) AS messages_invalid,
	SUM(messages_unplausible) AS messages_unplausible,
	SUM(messages_fake) AS messages_fake,
	SUM(messages_bad) AS messages_bad,
	SUM(messages_unconfirmed) AS messages_unconfirmed,
	SUM(messages_confirmed_indirectly) AS messages_confirmed_indirectly,
	SUM(messages_confirmed_directly) AS messages_confirmed_directly,
	
	MAX(distance_max) AS distance_max,
	MAX(distance_max_invalid) AS distance_max_invalid,
	MAX(distance_max_unplausible) AS distance_max_unplausible,
	MAX(distance_max_fake) AS distance_max_fake,
	MAX(distance_max_bad) AS distance_max_bad,
	MAX(distance_max_unconfirmed) AS distance_max_unconfirmed,
	MAX(distance_max_confirmed_indirectly) AS distance_max_confirmed_indirectly,
	MAX(distance_max_confirmed_directly) AS distance_max_confirmed_directly,

	SUM(buckets_5m) AS buckets_5m
FROM sender_position_statistics_15m
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW sender_statistics_1d
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	src_call,
	
	COUNT(*) AS receivers,

	MIN(ts_first) AS ts_first,
	MAX(ts_last) AS ts_last,
	
	SUM(messages) AS messages,
	SUM(messages_invalid) AS messages_invalid,
	SUM(messages_unplausible) AS messages_unplausible,
	SUM(messages_fake) AS messages_fake,
	SUM(messages_bad) AS messages_bad,
	SUM(messages_unconfirmed) AS messages_unconfirmed,
	SUM(messages_confirmed_indirectly) AS messages_confirmed_indirectly,
	SUM(messages_confirmed_directly) AS messages_confirmed_directly,
	
	MAX(distance_max) AS distance_max,
	MAX(distance_max_invalid) AS distance_max_invalid,
	MAX(distance_max_unplausible) AS distance_max_unplausible,
	MAX(distance_max_fake) AS distance_max_fake,
	MAX(distance_max_bad) AS distance_max_bad,
	MAX(distance_max_unconfirmed) AS distance_max_unconfirmed,
	MAX(distance_max_confirmed_indirectly) AS distance_max_confirmed_indirectly,
	MAX(distance_max_confirmed_directly) AS distance_max_confirmed_directly,

	SUM(buckets_5m) AS buckets_5m,
	SUM(buckets_15m) AS buckets_15m
FROM sender_position_statistics_1d
GROUP BY 1, 2
WITH NO DATA;

-- receiver statistics
CREATE MATERIALIZED VIEW receiver_statistics_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
AS
SELECT
	time_bucket('15 min', ts) AS ts,
	receiver,
	
	COUNT(*) AS senders,

	MIN(ts_first) AS ts_first,
	MAX(ts_last) AS ts_last,
	
	SUM(messages) AS messages,
	SUM(messages_invalid) AS messages_invalid,
	SUM(messages_unplausible) AS messages_unplausible,
	SUM(messages_fake) AS messages_fake,
	SUM(messages_bad) AS messages_bad,
	SUM(messages_unconfirmed) AS messages_unconfirmed,
	SUM(messages_confirmed_indirectly) AS messages_confirmed_indirectly,
	SUM(messages_confirmed_directly) AS messages_confirmed_directly,
	
	MAX(distance_max) AS distance_max,
	MAX(distance_max_invalid) AS distance_max_invalid,
	MAX(distance_max_unplausible) AS distance_max_unplausible,
	MAX(distance_max_fake) AS distance_max_fake,
	MAX(distance_max_bad) AS distance_max_bad,
	MAX(distance_max_unconfirmed) AS distance_max_unconfirmed,
	MAX(distance_max_confirmed_indirectly) AS distance_max_confirmed_indirectly,
	MAX(distance_max_confirmed_directly) AS distance_max_confirmed_directly,

	SUM(buckets_5m) AS buckets_5m
FROM sender_position_statistics_15m
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW receiver_statistics_1d
WITH (timescaledb.continuous)
AS
SELECT
	time_bucket('1 day', ts) AS ts,
	receiver,
	
	COUNT(*) AS senders,

	MIN(ts_first) AS ts_first,
	MAX(ts_last) AS ts_last,
	
	SUM(messages) AS messages,
	SUM(messages_invalid) AS messages_invalid,
	SUM(messages_unplausible) AS messages_unplausible,
	SUM(messages_fake) AS messages_fake,
	SUM(messages_bad) AS messages_bad,
	SUM(messages_unconfirmed) AS messages_unconfirmed,
	SUM(messages_confirmed_indirectly) AS messages_confirmed_indirectly,
	SUM(messages_confirmed_directly) AS messages_confirmed_directly,
	
	MAX(distance_max) AS distance_max,
	MAX(distance_max_invalid) AS distance_max_invalid,
	MAX(distance_max_unplausible) AS distance_max_unplausible,
	MAX(distance_max_fake) AS distance_max_fake,
	MAX(distance_max_bad) AS distance_max_bad,
	MAX(distance_max_unconfirmed) AS distance_max_unconfirmed,
	MAX(distance_max_confirmed_indirectly) AS distance_max_confirmed_indirectly,
	MAX(distance_max_confirmed_directly) AS distance_max_confirmed_directly,

	SUM(buckets_5m) AS buckets_5m,
	SUM(buckets_15m) AS buckets_15m
FROM sender_position_statistics_1d
GROUP BY 1, 2
WITH NO DATA;
