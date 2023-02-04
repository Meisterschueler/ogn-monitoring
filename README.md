# OGN monitoring

## Features
- [x] overview of the receivers health state
- [x] overview of the senders configuration
- [ ] receiver ranking (global and per country)
- [ ] sender ranking
- [ ] detailled receiver analysis
- [ ] detailled sender analysis

## Architecture

## Installation
### Configuration
Copy `.env.example` and rename it to `.env` then set the values accordingly.

### Docker images
Just run the makefile to create docker images.

`$ make`

### Deploy
`$ docker-compose --env-file .env up -d`
