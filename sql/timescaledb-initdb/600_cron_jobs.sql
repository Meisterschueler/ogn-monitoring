-- 'Continuous' updates
SELECT cron.schedule('10,25,40,55 * * * *', '-- plausibilisation
    SELECT update_plausibilities(
		(NOW()-INTERVAL''26 minutes'')::TIMESTAMP, 
		(NOW()-INTERVAL''5 minutes'')::TIMESTAMP,
		''1 hour''::INTERVAL
	);
	SELECT update_records(NOW() - INTERVAL''1 hour'', NOW());
	SELECT update_confirmations(NOW() - INTERVAL''1 hour'', NOW());
');

SELECT cron.schedule('*/15 * * * *', '-- receiver updates
	REFRESH MATERIALIZED VIEW CONCURRENTLY receivers;
	REFRESH MATERIALIZED VIEW CONCURRENTLY receivers_joined;
	REFRESH MATERIALIZED VIEW CONCURRENTLY ranking;
');

SELECT cron.schedule('*/5 * * * *', '-- sender updates
	REFRESH MATERIALIZED VIEW CONCURRENTLY senders;
	REFRESH MATERIALIZED VIEW CONCURRENTLY senders_joined;
');

SELECT cron.schedule('*/5 * * * *', '-- receiver_status_events update
	SELECT update_receiver_status_events(NOW() - INTERVAL''1 hour'', NOW());
');

-- daily updates
SELECT cron.schedule('10 0 * * *', '-- update relatives
	REFRESH MATERIALIZED VIEW CONCURRENTLY sender_relative_qualities;
	REFRESH MATERIALIZED VIEW CONCURRENTLY receiver_relative_qualities;
	REFRESH MATERIALIZED VIEW CONCURRENTLY duplicates;
');
