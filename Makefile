.PHONY: test run build docker-run

test:
	pytest

run:
	python -m week1_basics.src.main
	python -m week2_cdc.src.main

build:
	docker build -t senior-de-week1 .
	docker build -t senior-de-week2 .


docker-run:
	docker run -p 8000:8000 senior-de-week1
	docker run -p 8000:8000 senior-de-week2
