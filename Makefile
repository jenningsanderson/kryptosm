.PHONY: install install-dev test lint format clean help

# Default target
help:
	@echo "Available targets:"
	@echo "  install      - Install the package"
	@echo "  install-dev  - Install with dev dependencies"
	@echo "  test         - Run basic tests"
	@echo "  test-all     - Run all tests"
	@echo "  lint         - Run linting checks"
	@echo "  format       - Format code"
	@echo "  clean        - Clean build artifacts"

# Install the package
install:
	uv pip install -e .

# Install with dev dependencies
install-dev:
	uv pip install -e ".[dev]"

# Install all dependencies
install-all:
	uv pip install -e ".[all]"

# Run basic tests (no Spark required)
test:
	uv run pytest tests/test_imports.py -v

# Run Spark tests (requires Spark with network access)
test-spark:
	uv run pytest tests/ -v -m spark

# Run Parquet integration tests
test-parquet:
	uv run python tests/test_parquet_runner.py

# E2E tests (run in order, each can be run independently)
test-e2e-nodes:
	uv run python tests/test_e2e_nodes.py

test-e2e-ways:
	uv run python tests/test_e2e_ways.py

test-e2e-relations:
	uv run python tests/test_e2e_relations.py

test-e2e-all: test-e2e-nodes test-e2e-ways test-e2e-relations

# Run all tests
test-all:
	uv run pytest tests/ -v

# Run tests excluding Spark
test-no-spark:
	uv run pytest tests/ -v -m "not spark"

# Run tests with coverage
test-cov:
	uv run pytest tests/ -v --cov=kryptosm --cov-report=html

# Lint code
lint:
	uv run ruff check kryptosm tests
	uv run mypy kryptosm

# Format code
format:
	uv run black kryptosm tests
	uv run ruff check --fix kryptosm tests

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Sync dependencies
sync:
	uv sync

# Lock dependencies
lock:
	uv lock

# Build package
build:
	uv build

# Run the CLI with help
run-help:
	uv run kryptosm --help
