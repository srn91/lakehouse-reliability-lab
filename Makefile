.PHONY: build validate scaleout test lint verify clean serve

build:
	python3 -m app.cli build

validate:
	python3 -m app.cli validate

scaleout:
	python3 -m app.cli validate-scaleout

test:
	pytest -q

lint:
	ruff check app tests

verify: lint test build validate scaleout

serve:
	python3 -m uvicorn app.web:app --host 0.0.0.0 --port $${PORT:-8000}

clean:
	rm -rf warehouse
