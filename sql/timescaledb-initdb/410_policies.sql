SELECT add_continuous_aggregate_policy('sender_positions_5m',
  start_offset => INTERVAL '1 hour',
  end_offset => NULL,
  initial_start => '2000-01-01 00:00:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'5 minutes');
ALTER MATERIALIZED VIEW sender_positions_5m SET (timescaledb.compress = true);
SELECT add_compression_policy('sender_positions_5m', compress_after => INTERVAL '2 hours');

SELECT add_continuous_aggregate_policy('sender_positions_1h',
  start_offset => INTERVAL '2 hours',
  end_offset => NULL,
  initial_start => '2000-01-01 00:05:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 hour');
ALTER MATERIALIZED VIEW sender_positions_1h SET (timescaledb.compress = true);
SELECT add_compression_policy('sender_positions_1h', compress_after => INTERVAL '3 hours');

SELECT add_continuous_aggregate_policy('sender_positions_1d',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:10:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW sender_positions_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('sender_positions_1d', compress_after => INTERVAL '3 days');

SELECT add_continuous_aggregate_policy('sender_directions_1h',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:06:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 hour');
ALTER MATERIALIZED VIEW sender_directions_1h SET (timescaledb.compress = true);
SELECT add_compression_policy('sender_directions_1h', compress_after => INTERVAL '3 hours');

SELECT add_continuous_aggregate_policy('sender_directions_1d',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:11:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW sender_directions_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('sender_directions_1d', compress_after => INTERVAL '3 days');

SELECT add_continuous_aggregate_policy('records_1d',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:08:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 hour');
ALTER MATERIALIZED VIEW records_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('records_1d', compress_after => INTERVAL '3 days');
