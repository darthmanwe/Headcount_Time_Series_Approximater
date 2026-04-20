PY ?= python
PIP ?= $(PY) -m pip

.PHONY: install install-dev lint typecheck test test-cov run-api run-cli clean

install:
	$(PIP) install -e .

install-dev:
	$(PIP) install -e ".[dev]"

lint:
	$(PY) -m ruff check src tests apps

format:
	$(PY) -m ruff format src tests apps

typecheck:
	$(PY) -m mypy src/headcount

test:
	$(PY) -m pytest

test-cov:
	$(PY) -m pytest --cov=headcount --cov-report=term-missing

run-api:
	$(PY) -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8000

run-cli:
	$(PY) -m headcount.cli --help

clean:
	rm -rf build dist *.egg-info .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
