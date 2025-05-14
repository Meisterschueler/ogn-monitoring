# OGN monitoring
![Build Status](https://github.com/Meisterschueler/ogn-monitoring/actions/workflows/ci.yaml/badge.svg)

## Features
- [x] overview of the receivers health state
- [x] overview of the senders configuration
- [x] receiver ranking (global and per country)
- [x] detailled receiver analysis
- [x] detailled sender analysis
- [x] duplicate FLARM ID recognition

## Architecture

## Installation
### Configuration
Copy `.env.example` and rename it to `.env` then set the values accordingly.

### Docker images
Just run the makefile to create docker images.

`$ make`

### Deploy
`$ docker-compose --env-file .env up -d`
