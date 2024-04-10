ALTER TABLE positions SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'src_call',
	timescaledb.compress_chunk_time_interval = '1 day'
);
SELECT add_compression_policy('positions', compress_after => INTERVAL '1 day');

ALTER TABLE statuses SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'src_call',
	timescaledb.compress_chunk_time_interval = '20 days'
);
SELECT add_compression_policy('statuses', compress_after => INTERVAL '1 day');

ALTER TABLE invalids SET (
	timescaledb.compress,
	timescaledb.compress_chunk_time_interval = '20 days'
);
SELECT add_compression_policy('invalids', compress_after => INTERVAL '1 day');


ALTER TABLE events_receiver_status SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'src_call',
	timescaledb.compress_chunk_time_interval = '20 days'
);
SELECT add_compression_policy('events_receiver_status', compress_after => INTERVAL '1 day');

ALTER TABLE events_receiver_position SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'src_call',
	timescaledb.compress_chunk_time_interval = '20 days'
);
SELECT add_compression_policy('events_receiver_position', compress_after => INTERVAL '1 day');

ALTER TABLE events_sender_position SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'src_call',
	timescaledb.compress_chunk_time_interval = '20 days'
);
SELECT add_compression_policy('events_sender_position', compress_after => INTERVAL '1 day');
