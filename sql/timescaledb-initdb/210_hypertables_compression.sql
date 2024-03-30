ALTER TABLE positions SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'src_call',
	timescaledb.compress_chunk_time_interval = '24 hours'
);
SELECT add_compression_policy('positions', INTERVAL '2 hours');

ALTER TABLE statuses SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'src_call',
	timescaledb.compress_chunk_time_interval = '20 days'
);
SELECT add_compression_policy('statuses', INTERVAL '2 hours');

ALTER TABLE invalids SET (
	timescaledb.compress,
	timescaledb.compress_chunk_time_interval = '20 days'
);
SELECT add_compression_policy('invalids', INTERVAL '2 hours');


ALTER TABLE events_receiver_status SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'src_call',
	timescaledb.compress_chunk_time_interval = '20 days'
);
SELECT add_compression_policy('events_receiver_status', INTERVAL '2 hours');

ALTER TABLE events_receiver_position SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'src_call',
	timescaledb.compress_chunk_time_interval = '20 days'
);
SELECT add_compression_policy('events_receiver_position', INTERVAL '2 hours');

ALTER TABLE events_sender_position SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'src_call',
	timescaledb.compress_chunk_time_interval = '20 days'
);
SELECT add_compression_policy('events_sender_position', INTERVAL '2 hours');
