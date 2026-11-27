# Developer shortcuts for a POSIX shell. On Windows, invoke the Python tools directly.
PY ?= python

.PHONY: install lint format typecheck test cov integration package lock scan tf-check

install:
	$(PY) -m pip install -e ".[dev,azure,functions]" && pre-commit install

format:
	ruff format . && ruff check --fix .

lint:
	ruff format --check . && ruff check .

typecheck:
	mypy

test:
	pytest -m "not integration"

cov:
	pytest -m "not integration" --cov --cov-report=term-missing --cov-report=xml

integration:
	AZURITE=1 pytest -m integration

package:
	$(PY) tools/build_function_package.py

# Pins the Function App dependencies with hashes (the package ships this as requirements.txt).
# Run on Linux with Python 3.11, the Function App runtime: markers resolve per platform.
lock:
	$(PY) -c "import platform, sys; sys.exit(None if platform.system() == 'Linux' and sys.version_info[:2] == (3, 11) else 'run make lock on Linux with Python 3.11')"
	$(PY) -m piptools compile --quiet --generate-hashes --allow-unsafe --strip-extras --no-emit-index-url \
		--output-file functions/requirements.lock functions/requirements.txt

scan:
	@set -eu; \
	requirements=$$(mktemp); \
	trap 'rm -f "$$requirements"' EXIT; \
	$(PY) tools/secret_scan.py .; \
	bandit -c pyproject.toml -r src; \
	pip freeze --exclude-editable > "$$requirements"; \
	pip-audit --strict --no-deps --disable-pip -r "$$requirements"; \
	if [ -f functions/requirements.lock ]; then \
		pip-audit --strict --require-hashes --disable-pip -r functions/requirements.lock; \
	fi

tf-check:
	terraform -chdir=infra/envs/poc fmt -check -recursive && \
	terraform -chdir=infra/envs/poc init -backend=false && \
	terraform -chdir=infra/envs/poc validate && \
	terraform -chdir=infra/envs/poc test
