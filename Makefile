# ==============================================================================
# StreamWright Spark - Makefile
# ==============================================================================
#
# Primary Makefile for the StreamWright Spark distributed batch processing engine.
# Provides installation, packaging, unit testing, static job validation,
# and local Docker infrastructure orchestration (LocalStack, Postgres, Kafka,
# Hive Metastore, and Trino).
#
# ==============================================================================

# Active Python runtime: defaults to 'python' (respects active pyenv / virtualenv)
PYTHON ?= python
RUFF ?= ruff

# Package metadata
PACKAGE_NAME = streamwright-spark

# Installation mode:
#   prod - Standard non-editable package installation (pip install .) [DEFAULT]
#   dev  - Development editable mode with direct source symlinks (pip install -e .)
#   dist - Self-contained distribution install built from wheel/sdist in $(DIST_DIR)
MODE ?= prod

# Force dependency reinstallation: set to 'true' to force pip reinstall
FORCE_DEPS ?= false

# Temporary directory for building and staging distribution wheels and sdists
DIST_DIR = /tmp/sdist/streamwright-spark

.PHONY: help install uninstall verify clean clean-dist build lint test pytest \
	validate validate-order-files validate-partner-orders up down logs seed-order-files seed-partner-orders \
	lakehouse publish-s3 submit-order-files submit-partner-orders trino

# ==============================================================================
# Help & Information
# ==============================================================================

help:            ## show available commands
	@echo "StreamWright Spark - Available commands:"
	@echo ""
	@echo "Package & Testing:"
	@echo "  make install [MODE=prod|dev|dist]   - Install streamwright-spark (default: prod)"
	@echo "  make install FORCE_DEPS=true        - Force reinstall dependencies"
	@echo "  make build                          - Build distribution packages (wheel, sdist)"
	@echo "  make uninstall                      - Uninstall streamwright-spark"
	@echo "  make clean                          - Clean build and test artifacts"
	@echo "  make clean-dist                     - Clean distribution directory"
	@echo "  make verify                         - Verify streamwright-spark installation"
	@echo "  make lint                           - Check Ruff and Python code standards"
	@echo "  make test / make pytest             - Lint, then run unit tests with pytest"
	@echo ""
	@echo "Pipeline Validation:"
	@echo "  make validate                       - Validate all example jobs"
	@echo "  make validate-order-files           - Validate examples/kafka/order_files.yaml"
	@echo "  make validate-partner-orders        - Validate examples/s3/partner_orders.yaml with local overlay"
	@echo ""
	@echo "Docker Infrastructure & Local Services:"
	@echo "  make up                             - Start Postgres, LocalStack, and Kafka in Docker"
	@echo "  make down                           - Stop and remove all containers"
	@echo "  make seed-order-files               - Seed Kafka topics and S3 client files"
	@echo "  make seed-partner-orders            - Seed S3 buckets and databases for s3_partner_orders"
	@echo "  make lakehouse                      - Start Hive Metastore & Trino in Docker"
	@echo "  make publish-s3                     - Upload wheels and jobs to LocalStack S3"
	@echo "  make submit-order-files             - Run blind spark-submit for kafka_order_files"
	@echo "  make submit-partner-orders          - Run blind spark-submit for s3_partner_orders"
	@echo "  make trino                          - Launch interactive Trino CLI session"
	@echo "  make logs                           - Follow Docker Compose logs"
	@echo "  make help                           - Show this help message"
	@echo ""
	@echo "Installation modes:"
	@echo "  MODE=prod  - Production mode (pip install .[all]) [DEFAULT]"
	@echo "  MODE=dev   - Development mode (pip install -e .[all])"
	@echo "  MODE=dist  - Distribution mode (build + install from $(DIST_DIR))"

# ==============================================================================
# Package Lifecycle (Install, Uninstall, Build, Clean, Verify)
# ==============================================================================

# Install streamwright-spark into the active environment
# Supports MODE=prod (pip install .[all]), MODE=dev (pip install -e .[all]), or MODE=dist
install:         ## install streamwright-spark package
ifeq ($(MODE),dev)
	@echo "Installing $(PACKAGE_NAME) in development mode..."
	$(PYTHON) -m pip install -e ".[all]" $(if $(filter true,$(FORCE_DEPS)),--force-reinstall,)
else ifeq ($(MODE),prod)
	@echo "Installing $(PACKAGE_NAME) in production mode..."
	$(PYTHON) -m pip install ".[all]" $(if $(filter true,$(FORCE_DEPS)),--force-reinstall,)
else ifeq ($(MODE),dist)
	@echo "Installing $(PACKAGE_NAME) from distribution..."
	@$(MAKE) build
	@echo "Installing from distribution directory $(DIST_DIR)..."
	$(PYTHON) -m pip install "$(PACKAGE_NAME)[all]" --find-links $(DIST_DIR) --force-reinstall
else
	@echo "❌ Invalid MODE: $(MODE). Use dev, prod, or dist"
	@exit 1
endif
	@echo "✅ Installation complete!"
	@$(MAKE) verify

# Uninstall streamwright-spark from the current environment
uninstall:       ## uninstall streamwright-spark
	@echo "Uninstalling $(PACKAGE_NAME)..."
	$(PYTHON) -m pip uninstall -y $(PACKAGE_NAME) 2>/dev/null || true
	@echo "✅ $(PACKAGE_NAME) uninstalled!"

# Clean temporary compilation artifacts, bytecode, caches, and test outputs
clean:           ## clean build, dist, and pytest artifacts
	find . -type d \( -name "__pycache__" -o -name "*.egg-info" -o -name ".pytest_cache" -o -name "build" -o -name "dist" \) -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Build and test artifacts cleaned!"

# Clean distribution directory where build wheels and tarballs are staged
clean-dist:      ## clean distribution directory
	@rm -rf $(DIST_DIR)
	@echo "✅ Distribution directory cleaned!"

# Build standard distribution packages (wheel and sdist) into DIST_DIR
build: clean clean-dist  ## build distribution packages (wheel, sdist)
	@echo "Building distribution packages for $(PACKAGE_NAME)..."
	@mkdir -p $(DIST_DIR)
	$(PYTHON) -m build --outdir $(DIST_DIR)
	@echo "✅ Build complete! Files in $(DIST_DIR):"
	@ls -la $(DIST_DIR)/

# Verify that streamwright-spark is installed and its CLI is operational
verify:          ## verify streamwright-spark installation
	@echo "Verifying $(PACKAGE_NAME) installation..."
	@$(PYTHON) -m pip show $(PACKAGE_NAME) >/dev/null 2>&1 && echo "  ✅ $(PACKAGE_NAME) is installed!" || echo "  ❌ $(PACKAGE_NAME) is not installed!"
	@$(PYTHON) -c "import streamwright.spark; print('  ✅ streamwright.spark import works!')" 2>/dev/null || echo "  ❌ streamwright.spark import failed!"
	@streamwright-spark --help >/dev/null 2>&1 && echo "  ✅ streamwright-spark command works!" || echo "  ❌ streamwright-spark command failed!"

# ==============================================================================
# Testing
# ==============================================================================

# Ruff and the repository's dependency-free AST standards checker are blocking.
lint:            ## check all source and tests
	$(RUFF) check .
	$(PYTHON) scripts/check_code_standards.py --strict src tests

# Run the full unit test suite after the lint gate.
test: lint       ## run unit tests with pytest
	@if ! $(PYTHON) -c "import streamwright" 2>/dev/null; then \
	  echo "⚠️  Note: 'streamwright' package is not installed. Please install streamwright and needed connectors first."; \
	fi
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) -m pytest -p no:cacheprovider -q tests

# Run the full unit test suite using pytest for faster execution and summary reporting
pytest: test     ## lint and run unit tests with pytest

# ==============================================================================
# Pipeline Static Validation
# ==============================================================================

# Validate all example jobs statically without requiring active Spark or remote clusters
validate: validate-order-files validate-partner-orders  ## validate all example jobs

# Validate the kafka_order_files example job:
# Verifies connection schemas, two-phase staging tables, Delta Lake merge configs,
# event outbox triggers, and outputs the execution plan graph and estimated Spark actions.
validate-order-files:     ## validate examples/kafka/order_files.yaml
	$(PYTHON) -B -m streamwright.spark.cli validate examples/kafka/order_files.yaml --analyze --strict

# Validate the s3_partner_orders example job with local host overlay:
# Verifies heterogeneous partner prefix matching, regex extraction (path_pattern),
# embedded controlled vocabulary dictionaries, schema drift reports, and error routing.
validate-partner-orders: ## validate examples/s3/partner_orders.yaml with local overlay
	$(PYTHON) -B -m streamwright.spark.cli validate examples/s3/partner_orders.yaml --overlay examples/s3/partner_orders.local.yaml --analyze --strict

# ==============================================================================
# Local Infrastructure & Docker Services
# ==============================================================================

# Start base local backing services in background containers:
#   - LocalStack: Mock AWS S3 (landing, lake, and code buckets)
#   - PostgreSQL: Stores state store locks, batch leases, and relational audit tables
#   - Kafka + Zookeeper: Event notification message bus and outbox triggers
up:              ## start Postgres, LocalStack, and Kafka in Docker
	docker compose up -d --wait

# Tear down all Docker containers, networks, and persistent volumes (clean wipe)
down:            ## stop and remove all containers
	docker compose --profile lakehouse down -v

# Follow real-time unified logs from all active Docker Compose services
logs:            ## follow Docker Compose logs
	docker compose logs -f

# Seed mock test data for kafka_order_files:
# Creates the Kafka topic 'events.batch.done', generates compressed mock client CSVs,
# uploads files to LocalStack S3 landing bucket, and sends event triggers to Kafka.
seed-order-files:            ## seed Kafka topics and S3 client files
	sh docker/seed_order_files.sh

# Seed mock test data for s3_partner_orders:
# Provisions S3 landing and lake buckets, generates sample feeds for Partner Alpha,
# Partner Beta (with schema drift), and intentional error rows, and pre-populates Postgres.
seed-partner-orders:     ## seed S3 buckets and databases for s3_partner_orders
	sh docker/seed_partner_orders.sh

# Start the lakehouse profile services alongside Postgres, LocalStack, and Kafka:
#   - Hive Metastore: Manages Delta Lake catalog definitions and schema metadata
#   - Trino: Distributed ANSI SQL query engine for verifying analytical Delta tables
lakehouse:       ## start Hive Metastore & Trino in Docker
	docker compose --profile lakehouse up -d --wait

# Build wheel distributions and upload entrypoint, packages, and job definitions
# directly into LocalStack S3 (s3://code-bucket/) to prepare for blind submission
publish-s3:      ## upload wheels and jobs to LocalStack S3
	sh docker/publish_s3.sh

# Run blind spark-submit in an isolated container for kafka_order_files:
# Emulates production cloud execution by resolving wheels and YAMLs exclusively from S3.
submit-order-files:    ## run blind spark-submit for kafka_order_files
	sh docker/submit_order_files.sh

# Run blind spark-submit in an isolated container for s3_partner_orders:
# Executes end-to-end partner feed harmonization, drift audit, and Delta table merge in Docker.
submit-partner-orders:   ## run blind spark-submit for s3_partner_orders
	sh docker/submit_partner_orders.sh

# Launch an interactive Trino CLI session connected to the local Delta Lake catalog:
# Enables ad-hoc SQL querying against Lakehouse tables (e.g. analytics.orders)
trino:           ## launch interactive Trino CLI session
	docker compose --profile lakehouse exec trino trino --catalog delta --schema analytics
