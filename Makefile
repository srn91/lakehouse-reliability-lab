.PHONY: build validate test lint verify clean serve

build:
	python3 -m app.cli build

validate:
	python3 -m app.cli validate

test:
	pytest -q

lint:
	ruff check app tests

verify: lint test build validate

serve:
	python3 -m uvicorn app.web:app --host 0.0.0.0 --port $${PORT:-8000}

clean:
	rm -rf warehouse
