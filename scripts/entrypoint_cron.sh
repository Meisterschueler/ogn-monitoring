#!/bin/bash

echo "@daily /scripts/update_ddb.sh" >> /var/spool/cron/crontabs/root

crond -f -l 2 -L /dev/stdout
