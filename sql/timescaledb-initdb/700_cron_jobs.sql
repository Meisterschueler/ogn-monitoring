-- 'Continuous' updates
SELECT cron.schedule('*/5 * * * *', '-- regular 5min. update
    SELECT update_receivers();
    SELECT update_senders();
    SELECT update_receiver_countries();
    SELECT update_plausibilities(
		(NOW()-INTERVAL''11 minutes'')::TIMESTAMP, 
		(NOW()-INTERVAL''5 minutes'')::TIMESTAMP
	);
');
SELECT cron.schedule('*/5 * * * *', 'REFRESH MATERIALIZED VIEW senders_joined;');
SELECT cron.schedule('*/5 * * * *', 'REFRESH MATERIALIZED VIEW receivers_joined;');
SELECT cron.schedule('*/5 * * * *', 'REFRESH MATERIALIZED VIEW ranking;');

-- 'Daily' updates
SELECT cron.schedule('10 0 * * *', 'REFRESH MATERIALIZED VIEW senders_relative_qualities;');
