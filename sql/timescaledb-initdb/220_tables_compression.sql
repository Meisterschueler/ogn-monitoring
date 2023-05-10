ALTER TABLE positions SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'src_call',
	timescaledb.compress_chunk_time_interval = '24 hours'
);
SELECT add_compression_policy('positions', INTERVAL '2 days');

ALTER TABLE statuses SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'src_call',
	timescaledb.compress_chunk_time_interval = '24 hours'
);
SELECT add_compression_policy('statuses', INTERVAL '2 days');
