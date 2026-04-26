.PHONY: build validate test lint verify clean

build:
	python3 -m app.cli build

validate:
	python3 -m app.cli validate

test:
	pytest -q

lint:
	ruff check app tests

verify: lint test validate

clean:
	rm -rf warehouse

