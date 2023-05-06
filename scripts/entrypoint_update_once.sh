#!/bin/bash

echo === Started entrypoint_update_once.sh ===

sh ./update_countries.sh
sh ./update_openaip.sh
sh ./update_ressources.sh

echo === Finished entrypoint_update_once.sh ===
