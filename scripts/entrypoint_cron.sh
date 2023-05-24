#!/bin/bash

echo === Started entrypoint_cron.sh ===

# Initial execution of the scripts
sh /scripts/update_ddb.sh
sh /scripts/update_opensky.sh
sh /scripts/update_weglide.sh

# Configure cron for repeated execution
echo "@daily /scripts/update_ddb.sh" >> /var/spool/cron/crontabs/root
echo "@daily /scripts/update_opensky.sh" >> /var/spool/cron/crontabs/root
echo "@daily /scripts/update_weglide.sh" >> /var/spool/cron/crontabs/root

crond -f -L /dev/stdout

echo === Finished entrypoint_cron.sh ===
