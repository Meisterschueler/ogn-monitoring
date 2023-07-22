-- create a function which updates receiver_status_events and returns the proceeded rows
CREATE OR REPLACE FUNCTION update_receiver_status_events(lower TIMESTAMPTZ, upper TIMESTAMPTZ)
RETURNS INTEGER AS $$
DECLARE
	processed_position_rows INTEGER;
BEGIN

EXECUTE '
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
            WHERE ts BETWEEN TIMESTAMP''' || lower || ''' - INTERVAL''1 hour'' AND TIMESTAMP''' || upper || '''
        ) AS sq
    ) AS sq2
    WHERE
        sq2.event != 0
        AND sq2.ts BETWEEN TIMESTAMP''' || lower || ''' AND TIMESTAMP''' || upper || '''
';

	GET DIAGNOSTICS processed_position_rows = ROW_COUNT;
	RETURN processed_position_rows;

END;
$$ LANGUAGE plpgsql;
