-- 'Continuous' updates
SELECT cron.schedule('10,25,40,55 * * * *', '-- plausibilisation
    SELECT update_plausibilities(
		(NOW()-INTERVAL''26 minutes'')::TIMESTAMP, 
		(NOW()-INTERVAL''5 minutes'')::TIMESTAMP,
		''1 hour''::INTERVAL
	);
	SELECT update_records(NOW() - INTERVAL''1 hour'', NOW());
');

SELECT cron.schedule('5,10,15,20,25,30,35,40,45,50,55 * * * *', '-- receiver updates
	REFRESH MATERIALIZED VIEW CONCURRENTLY receivers;
	REFRESH MATERIALIZED VIEW CONCURRENTLY receivers_joined;
	REFRESH MATERIALIZED VIEW CONCURRENTLY ranking;
');

SELECT cron.schedule('6,11,16,21,26,31,36,41,46,51,56 * * * *', '-- sender updates
	REFRESH MATERIALIZED VIEW CONCURRENTLY senders;
	REFRESH MATERIALIZED VIEW CONCURRENTLY senders_joined;
');

SELECT cron.schedule('7,12,17,22,27,32,37,42,47,52,57 * * * *', '-- logbook updates
	SELECT update_events_takeoff(NOW() - INTERVAL''10 minutes'', NOW());
	SELECT update_takeoffs(NOW() - INTERVAL''20 minutes'', NOW());
	REFRESH MATERIALIZED VIEW logbook;
');

SELECT cron.schedule('7 * * * *', '-- event updates
	SELECT update_events_receiver_status(INTERVAL''1 day'');
	SELECT update_events_receiver_position(INTERVAL''1 day'');
	SELECT update_events_sender_position(INTERVAL''1 day'');
');

-- daily updates
SELECT cron.schedule('8 0 * * *', '-- update relatives
	REFRESH MATERIALIZED VIEW CONCURRENTLY sender_relative_qualities;
	REFRESH MATERIALIZED VIEW CONCURRENTLY receiver_relative_qualities;
	REFRESH MATERIALIZED VIEW CONCURRENTLY duplicates;
');

SELECT cron.schedule('9 0 * * *', '-- update confirmations
	SELECT update_confirmations(NOW()::DATE);
');
