# Use the pyenv environment: `pyenv activate streamwright` (see .python-version), or set PYTHON.
PYTHON ?= python
# Where the StreamWright repo is, for the connectors the tests and the Docker image use (the hub is the real source:
# `streamwright connectors install s3 postgres deltalake kafka sqs`; kafka and sqs reach the hub with the first release tag).
STREAMWRIGHT ?= ../streamwright
CONNECTORS ?= files s3 gcs azure_blob postgres deltalake kafka sqs
# For a checkout without installed packages: the sources of streamwright (core) and its connectors
empty :=
space := $(empty) $(empty)
SRC_PATH = src:$(STREAMWRIGHT)/core/src:$(subst $(space),:,$(addsuffix /src,$(addprefix $(STREAMWRIGHT)/connectors/readers/,$(CONNECTORS))))

.PHONY: install test validate up down logs seed
install:         ## into the active pyenv env: streamwright core, the connectors, then this package (not run yet)
	$(PYTHON) -m pip install -e $(STREAMWRIGHT)/core
	$(PYTHON) -m pip install $(foreach c,$(CONNECTORS),$(STREAMWRIGHT)/connectors/readers/$(c))
	$(PYTHON) -m pip install -e ".[all]"

test:            ## unit tests (no Spark or Docker needed); uses the installed packages, or the sources next door
	@if $(PYTHON) -c "import streamwright.spark, streamwright.connectors.s3" 2>/dev/null; then \
	  $(PYTHON) -B -m unittest discover -s tests -v; \
	else PYTHONPATH=$(SRC_PATH) $(PYTHON) -B -m unittest discover -s tests -v; fi

validate:        ## check the example job
	$(PYTHON) -B -m streamwright.spark.cli validate examples/s3_events_to_delta --analyze --strict

up:              ## start Postgres, LocalStack and Kafka (copy .env.example to .env first)
	docker compose up -d --wait

seed:            ## Kafka topics, sample client files and messages (after `make up`)
	sh docker/seed.sh

down:            ## stop and remove all containers, networks, and volumes (clean wipe)
	docker compose --profile lakehouse down -v

lakehouse:       ## start Hive Metastore & Trino alongside Postgres, LocalStack, and Kafka
	docker compose --profile lakehouse up -d --wait

publish-s3:      ## upload packages, entrypoint, and job definitions to LocalStack S3
	sh docker/publish_s3.sh

submit-blind:    ## run blind spark-submit in clean container using S3-hosted artifacts
	sh docker/submit_blind.sh

trino:           ## launch interactive Trino CLI session
	docker compose --profile lakehouse exec trino trino --catalog delta --schema analytics

logs:
	docker compose logs -f

