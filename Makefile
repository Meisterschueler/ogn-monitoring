docker:
	docker build -t ogn-client-rs -f Dockerfile.ogn-client-rs .
	docker build -t countries -f Dockerfile.countries .
	docker build -t ressources -f Dockerfile.ressources .
	docker build -t openaip -f Dockerfile.openaip .
