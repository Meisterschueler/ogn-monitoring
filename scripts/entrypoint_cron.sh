#!/bin/bash

echo === Started entrypoint_cron.sh ===

echo "@daily /scripts/update_ddb.sh" >> /var/spool/cron/crontabs/root
echo "@daily /scripts/update_opensky.sh" >> /var/spool/cron/crontabs/root
echo "@daily /scripts/update_weglide.sh" >> /var/spool/cron/crontabs/root

crond -f -L /dev/stdout

echo === Finished entrypoint_cron.sh ===
