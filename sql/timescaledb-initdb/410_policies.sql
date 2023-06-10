SELECT add_continuous_aggregate_policy('positions_5m',
  start_offset => INTERVAL '10 minutes',
  end_offset => NULL,
  initial_start => '2000-01-01 00:00:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'5 minutes');
ALTER MATERIALIZED VIEW positions_5m SET (timescaledb.compress = true);
SELECT add_compression_policy('positions_5m', compress_after => INTERVAL '15 minutes');

SELECT add_continuous_aggregate_policy('positions_1h',
  start_offset => INTERVAL '2 hours',
  end_offset => NULL,
  initial_start => '2000-01-01 00:05:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 hour');
ALTER MATERIALIZED VIEW positions_1h SET (timescaledb.compress = true);
SELECT add_compression_policy('positions_1h', compress_after => INTERVAL '3 hours');

SELECT add_continuous_aggregate_policy('positions_1d',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:10:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW positions_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('positions_1d', compress_after => INTERVAL '3 days');

SELECT add_continuous_aggregate_policy('direction_statistics_1d',
  start_offset => INTERVAL '2 days',
  end_offset => NULL,
  initial_start => '2000-01-01 00:20:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW direction_statistics_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('direction_statistics_1d', compress_after => INTERVAL '3 days');
