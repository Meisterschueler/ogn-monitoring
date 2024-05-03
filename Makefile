docker:
	docker build --no-cache -t ogn-client-rs -f Dockerfile.ogn-client-rs .
	docker build --no-cache -t ressources -f Dockerfile.ressources .
	docker build --no-cache -t nodejs -f Dockerfile.nodejs .
