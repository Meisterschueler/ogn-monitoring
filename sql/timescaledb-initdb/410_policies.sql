-- sender positions
SELECT add_continuous_aggregate_policy('sender_positions_5m',
  start_offset => INTERVAL '1 hour',
  end_offset => NULL,
  initial_start => '2000-01-01 00:00:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'5 minutes');
ALTER MATERIALIZED VIEW sender_positions_5m SET (timescaledb.compress = true);
SELECT add_compression_policy('sender_positions_5m', compress_after => INTERVAL '2 hours');

-- directions
SELECT add_continuous_aggregate_policy('directions_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => NULL,
  initial_start => '2000-01-01 00:02:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '15 minutes');
ALTER MATERIALIZED VIEW directions_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('directions_15m', compress_after => INTERVAL '3 hours');

SELECT add_continuous_aggregate_policy('directions_1d',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:05:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW directions_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('directions_1d', compress_after => INTERVAL '3 days');

-- sender position states
SELECT add_continuous_aggregate_policy('sender_position_states_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => NULL,
  initial_start => '2000-01-01 00:02:30'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'15 minutes');
ALTER MATERIALIZED VIEW sender_position_states_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('sender_position_states_15m', compress_after => INTERVAL '3 hours');

SELECT add_continuous_aggregate_policy('sender_position_states_1d',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:06:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW sender_position_states_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('sender_position_states_1d', compress_after => INTERVAL '3 days');

-- receiver position states
SELECT add_continuous_aggregate_policy('receiver_position_states_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => NULL,
  initial_start => '2000-01-01 00:03:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '15 minutes');
ALTER MATERIALIZED VIEW receiver_position_states_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('receiver_position_states_15m', compress_after => INTERVAL '3 hours');

SELECT add_continuous_aggregate_policy('receiver_position_states_1d',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:07:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW receiver_position_states_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('receiver_position_states_1d', compress_after => INTERVAL '3 days');

-- receiver status states
SELECT add_continuous_aggregate_policy('receiver_status_states_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => NULL,
  initial_start => '2000-01-01 00:03:30'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '15 minutes');
ALTER MATERIALIZED VIEW receiver_status_states_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('receiver_status_states_15m', compress_after => INTERVAL '3 hours');

SELECT add_continuous_aggregate_policy('receiver_status_states_1d',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:08:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW receiver_status_states_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('receiver_status_states_1d', compress_after => INTERVAL '3 days');

-- sender position statistics
SELECT add_continuous_aggregate_policy('sender_position_statistics_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => NULL,
  initial_start => '2000-01-01 00:04:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '15 minutes');
ALTER MATERIALIZED VIEW sender_position_statistics_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('sender_position_statistics_15m', compress_after => INTERVAL '3 hours');

SELECT add_continuous_aggregate_policy('sender_position_statistics_1d',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:09:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW sender_position_statistics_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('sender_position_statistics_1d', compress_after => INTERVAL '3 days');

-- sender statistics
SELECT add_continuous_aggregate_policy('sender_statistics_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => NULL,
  initial_start => '2000-01-01 00:04:30'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '15 minutes');
ALTER MATERIALIZED VIEW sender_statistics_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('sender_statistics_15m', compress_after => INTERVAL '3 hours');

SELECT add_continuous_aggregate_policy('sender_statistics_1d',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:10:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW sender_statistics_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('sender_statistics_1d', compress_after => INTERVAL '3 days');

-- receiver statistics
SELECT add_continuous_aggregate_policy('receiver_statistics_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => NULL,
  initial_start => '2000-01-01 00:05:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '15 minutes');
ALTER MATERIALIZED VIEW receiver_statistics_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('receiver_statistics_15m', compress_after => INTERVAL '3 hours');

SELECT add_continuous_aggregate_policy('receiver_statistics_1d',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:11:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW receiver_statistics_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('receiver_statistics_1d', compress_after => INTERVAL '3 days');