-- 'Continuous' updates
SELECT cron.schedule('*/5 * * * *', 'SELECT update_receivers();');
SELECT cron.schedule('*/5 * * * *', 'SELECT update_senders();');

SELECT cron.schedule('*/5 * * * *', 'SELECT update_receiver_countries();');

SELECT cron.schedule('*/5 * * * *', 'REFRESH MATERIALIZED VIEW senders_joined;');

-- 'Daily' updates
SELECT cron.schedule('10 0 * * *', 'REFRESH MATERIALIZED VIEW senders_relative_qualities;');
