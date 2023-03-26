SELECT add_continuous_aggregate_policy('positions_5m',
  start_offset => INTERVAL'5 days',
  end_offset => INTERVAL'5 minutes',
  schedule_interval => INTERVAL'5 minutes');

SELECT add_continuous_aggregate_policy('ranking_statistics_5m',
  start_offset => NULL,
  end_offset => INTERVAL '5 minutes',
  schedule_interval => INTERVAL '5 minutes');

SELECT add_continuous_aggregate_policy('ranking_statistics_1h',
  start_offset => NULL,
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('ranking_statistics_1d',
  start_offset => NULL,
  end_offset => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 day');

SELECT add_continuous_aggregate_policy('direction_statistics_1d',
  start_offset => NULL,
  end_offset => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 day');
