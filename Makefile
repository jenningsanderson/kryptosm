.PHONY: install install-dev sync lock build lint format clean help \
        test-e2e-nodes test-e2e-ways test-e2e-relations test-e2e-osc \
        test-e2e-init test-e2e test-e2e-all

help:
	@echo "Available targets:"
	@echo "  install            Install the package"
	@echo "  install-dev        Install with dev dependencies"
	@echo "  test-e2e           Init + OSC apply, single Spark session, timed"
	@echo "  test-e2e-init      Full init pipeline (nodes+ways+relations) with timing"
	@echo "  test-e2e-nodes     E2E stage 1: build nodes"
	@echo "  test-e2e-ways      E2E stage 2: build ways"
	@echo "  test-e2e-relations E2E stage 3: build relations"
	@echo "  test-e2e-osc       E2E stage 4: apply OSC update"
	@echo "  test-e2e-all       Run all E2E stages in order"
	@echo "  lint               Ruff + mypy"
	@echo "  format             Black + ruff --fix"
	@echo "  clean              Remove build artifacts"

install:
	uv pip install -e .

install-dev:
	uv pip install -e ".[dev]"

sync:
	uv sync

lock:
	uv lock

build:
	uv build

# E2E stages each persist their output to tests/data/output/warehouse and
# can be run independently after their predecessors.
test-e2e-nodes:
	uv run python tests/test_e2e_nodes.py -v -s

test-e2e-ways:
	uv run python tests/test_e2e_ways.py -v -s

test-e2e-relations:
	uv run python tests/test_e2e_relations.py -v -s

test-e2e-osc:
	uv run python tests/test_e2e_osc.py -v -s

test-e2e-init:
	uv run python tests/test_e2e_init.py -v -s

test-e2e:
	uv run python tests/test_e2e.py -v -s

test-e2e-all: test-e2e-nodes test-e2e-ways test-e2e-relations test-e2e-osc

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
