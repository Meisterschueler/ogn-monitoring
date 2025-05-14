-- positions_5m related
SELECT add_continuous_aggregate_policy('positions_5m',
  start_offset => INTERVAL '1 hour',
  end_offset => INTERVAL '5 minutes',
  initial_start => '2000-01-01 00:00:00'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'5 minutes');
ALTER MATERIALIZED VIEW positions_5m SET (timescaledb.compress = true);
SELECT add_compression_policy('positions_5m', compress_after => INTERVAL '2 hours');

SELECT add_continuous_aggregate_policy('statistics_sender_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '15 minutes',
  initial_start => '2000-01-01 00:00:35'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'15 minutes');
ALTER MATERIALIZED VIEW statistics_sender_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('statistics_sender_15m', compress_after => INTERVAL '4 hours');

SELECT add_continuous_aggregate_policy('statistics_receiver_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '15 minutes',
  initial_start => '2000-01-01 00:00:40'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'15 minutes');
ALTER MATERIALIZED VIEW statistics_receiver_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('statistics_receiver_15m', compress_after => INTERVAL '4 hours');


-- directions
SELECT add_continuous_aggregate_policy('directions_1h',
  start_offset => INTERVAL '8 hours',
  end_offset => INTERVAL '1 hour',
  initial_start => '2000-01-01 00:00:20'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 hour');
ALTER MATERIALIZED VIEW directions_1h SET (timescaledb.compress = true);
SELECT add_compression_policy('directions_1h', compress_after => INTERVAL '16 hours');

SELECT add_continuous_aggregate_policy('directions_1d',
  start_offset => INTERVAL '8 days',
  end_offset => INTERVAL '1 day',
  initial_start => '2000-01-01 00:01:10'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW directions_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('directions_1d', compress_after => INTERVAL '16 days');

-- extra view
SELECT add_continuous_aggregate_policy('positions_sender_original_address_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '15 minutes',
  initial_start => '2000-01-01 00:00:45'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'15 minutes');
ALTER MATERIALIZED VIEW positions_sender_original_address_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('positions_sender_original_address_15m', compress_after => INTERVAL '4 hours');

SELECT add_continuous_aggregate_policy('positions_sender_original_address_1d',
  start_offset => INTERVAL '8 days',
  end_offset => INTERVAL '1 day',
  initial_start => '2000-01-01 00:01:15'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW positions_sender_original_address_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('positions_sender_original_address_1d', compress_after => INTERVAL '16 days');

SELECT add_continuous_aggregate_policy('statistics_dst_call_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '15 minutes',
  initial_start => '2000-01-01 00:00:50'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'15 minutes');
ALTER MATERIALIZED VIEW statistics_dst_call_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('statistics_dst_call_15m', compress_after => INTERVAL '4 hours');


-- statuses
SELECT add_continuous_aggregate_policy('statuses_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '15 minutes',
  initial_start => '2000-01-01 00:00:55'::TIMESTAMPTZ,
  schedule_interval => INTERVAL'15 minutes');
ALTER MATERIALIZED VIEW statuses_15m SET (timescaledb.compress = true);
SELECT add_compression_policy('statuses_15m', compress_after => INTERVAL '4 hours');

SELECT add_continuous_aggregate_policy('statuses_1d',
  start_offset => INTERVAL '8 days',
  end_offset => INTERVAL '1 day',
  initial_start => '2000-01-01 00:01:20'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW statuses_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('statuses_1d', compress_after => INTERVAL '16 days');

SELECT add_continuous_aggregate_policy('statuses_sender_1d',
  start_offset => INTERVAL '8 days',
  end_offset => INTERVAL '1 day',
  initial_start => '2000-01-01 00:01:25'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW statuses_sender_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('statuses_sender_1d', compress_after => INTERVAL '16 days');

SELECT add_continuous_aggregate_policy('statuses_receiver_1d',
  start_offset => INTERVAL '8 days',
  end_offset => INTERVAL '1 day',
  initial_start => '2000-01-01 00:01:30'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW statuses_receiver_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('statuses_receiver_1d', compress_after => INTERVAL '16 days');

-- online
SELECT add_continuous_aggregate_policy('online_receiver_1d',
  start_offset => INTERVAL '8 days',
  end_offset => INTERVAL '1 day',
  initial_start => '2000-01-01 00:01:35'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW online_receiver_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('online_receiver_1d', compress_after => INTERVAL '16 days');

SELECT add_continuous_aggregate_policy('online_sender_1d',
  start_offset => INTERVAL '8 days',
  end_offset => INTERVAL '1 day',
  initial_start => '2000-01-01 00:01:40'::TIMESTAMPTZ,
  schedule_interval => INTERVAL '1 day');
ALTER MATERIALIZED VIEW online_sender_1d SET (timescaledb.compress = true);
SELECT add_compression_policy('online_sender_1d', compress_after => INTERVAL '16 days');