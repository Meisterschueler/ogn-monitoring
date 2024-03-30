-- sender positions
SELECT add_continuous_aggregate_policy('positions_5m',
  start_offset => INTERVAL '1 hour',
  end_offset => INTERVAL '5 minutes',
  initial_start => '2000-01-01 00:00:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'5 minutes');
ALTER MATERIALIZED VIEW positions_5m SET (timescaledb.compress = true);
SELECT add_compression_policy('positions_5m', compress_after => INTERVAL '2 hours');

SELECT add_continuous_aggregate_policy('positions_1d',
  start_offset => INTERVAL '8 days',
  end_offset => INTERVAL '1 day',
  initial_start => '2000-01-01 00:05:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW positions_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('positions_1d', compress_after => INTERVAL '16 days');

SELECT add_continuous_aggregate_policy('positions_sender_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '15 minutes',
  initial_start => '2000-01-01 00:02:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '15 minutes');
ALTER MATERIALIZED VIEW positions_sender_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('positions_sender_15m', compress_after => INTERVAL '4 hours');

SELECT add_continuous_aggregate_policy('positions_receiver_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '15 minutes',
  initial_start => '2000-01-01 00:02:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '15 minutes');
ALTER MATERIALIZED VIEW positions_receiver_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('positions_receiver_15m', compress_after => INTERVAL '4 hours');

-- directions
SELECT add_continuous_aggregate_policy('directions_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '15 minutes',
  initial_start => '2000-01-01 00:02:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '15 minutes');
ALTER MATERIALIZED VIEW directions_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('directions_15m', compress_after => INTERVAL '4 hours');

SELECT add_continuous_aggregate_policy('directions_1d',
  start_offset => INTERVAL '8 days',
  end_offset => INTERVAL '1 day',
  initial_start => '2000-01-01 00:05:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW directions_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('directions_1d', compress_after => INTERVAL '16 days');

-- receiver dst_call statistics
SELECT add_continuous_aggregate_policy('statistics_receiver_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '15 minutes',
  initial_start => '2000-01-01 00:02:30'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'15 minutes');
ALTER MATERIALIZED VIEW statistics_receiver_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('statistics_receiver_15m', compress_after => INTERVAL '4 hours');

SELECT add_continuous_aggregate_policy('statistics_sender_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '15 minutes',
  initial_start => '2000-01-01 00:02:30'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'15 minutes');
ALTER MATERIALIZED VIEW statistics_sender_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('statistics_sender_15m', compress_after => INTERVAL '4 hours');

SELECT add_continuous_aggregate_policy('positions_sender_original_address_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '15 minutes',
  initial_start => '2000-01-01 00:02:30'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'15 minutes');
ALTER MATERIALIZED VIEW positions_sender_original_address_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('positions_sender_original_address_15m', compress_after => INTERVAL '4 hours');