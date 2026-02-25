.PHONY: test run build docker-run docker-run-prometheus

test:
	pytest

run:
	python -m week1_basics.src.main

build:
	docker build -t senior-de-week1 .

docker-run:
	docker run --rm senior-de-week1

docker-run-prometheus:
	docker run -p 8000:8000 senior-de-week1