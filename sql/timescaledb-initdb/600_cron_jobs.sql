-- 'Continuous' updates
SELECT cron.schedule('10,25,40,55 * * * *', '-- plausibilisation
    SELECT update_plausibilities(
		(NOW()-INTERVAL''26 minutes'')::TIMESTAMP, 
		(NOW()-INTERVAL''5 minutes'')::TIMESTAMP
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

SELECT cron.schedule('*/5 * * * *', '-- receiver event update
	INSERT INTO receiver_status_events(ts, src_call, event, description)
	SELECT
		*
	FROM (
		SELECT
			sq.ts,
			sq.src_call,

			-- event
			-- bit 0: reboot
			-- bit 1: server change
			0
			+ CASE WHEN sq.senders_messages IS NOT NULL AND (sq.senders_messages_prev IS NULL OR sq.senders_messages < sq.senders_messages_prev) THEN 1 ELSE 0 END
			+ CASE WHEN sq.receiver IS NOT NULL AND (sq.receiver_prev IS NULL OR sq.receiver != sq.receiver_prev) THEN 2 ELSE 0 END
			AS event,
		
			CASE
				WHEN sq.receiver IS NOT NULL AND sq.receiver_prev IS NULL THEN ''to '' || sq.receiver
				WHEN sq.receiver IS NOT NULL AND sq.receiver != sq.receiver_prev THEN sq.receiver_prev || '' to '' || sq.receiver
				ELSE NULL
			END AS desc
		FROM (
			SELECT
				ts,
				src_call,
				senders_messages,
				receiver,
				LAG(senders_messages) OVER (PARTITION BY src_call ORDER BY ts) AS senders_messages_prev,
				LAG(receiver) OVER (PARTITION BY src_call ORDER BY ts) AS receiver_prev
			FROM statuses
			WHERE ts > (SELECT COALESCE(LAST(ts, ts), TIMESTAMP''2000-01-01 00:00:00'') - INTERVAL''1 hour'' FROM receiver_status_events)
		) AS sq
	) AS sq2
	WHERE
		sq2.event != 0
		AND sq2.ts > (SELECT COALESCE(LAST(ts, ts), TIMESTAMP''2000-01-01 00:00:00'') FROM receiver_status_events);
');

-- hourly updates
SELECT cron.schedule('10 0 * * *', '-- update relatives
	REFRESH MATERIALIZED VIEW CONCURRENTLY senders_relative_qualities;
	REFRESH MATERIALIZED VIEW CONCURRENTLY receivers_relative_qualities;
');
