.PHONY: test run build docker-run

test:
	pytest

run:
	python -m week1_basics.src.main

build:
	docker build -t senior-de-week1 .

docker-run:
	docker run --rm senior-de-week1