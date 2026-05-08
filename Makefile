.PHONY: sync lock build lint format clean help \
        test-e2e-init test-e2e-osc test-e2e-osc-all \
        test-replication test-inspect

help:
	@echo "Available targets:"
	@echo "  test-e2e-init      Full init pipeline (nodes+ways+relations)"
	@echo "  test-e2e-osc       Fetch + apply the next pending OSC (idempotent)"
	@echo "  test-e2e-osc-all   Fetch + apply ALL pending OSCs until current"
	@echo "  test-inspect       Snapshot inspector"
	@echo "  test-replication   Replication unit + live tests"
	@echo "  lint               Ruff + mypy"
	@echo "  format             Black + ruff --fix"
	@echo "  clean              Remove build artifacts"

sync:
	uv sync

lock:
	uv lock

build:
	uv build

test-e2e-init:
	uv run python tests/test_e2e_init.py -v -s

test-e2e-osc:
	uv run pytest tests/test_e2e_osc.py -m integration -v -s

test-e2e-osc-all:
	uv run pytest tests/test_e2e_osc_all.py -m integration -v -s

test-replication:
	uv run pytest tests/test_replication.py -v

test-inspect:
	uv run python tests/test_inspect.py -v -s

lint:
	uv run ruff check kryptosm tests
	uv run mypy kryptosm

format:
	uv run black kryptosm tests
	uv run ruff check --fix kryptosm tests

clean:
	rm -rf build dist *.egg-info .pytest_cache .coverage htmlcov \
	       .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
