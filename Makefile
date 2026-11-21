# CDC lakehouse: Postgres -> Debezium -> Kafka -> Iceberg (exactly-once) -> dbt SCD2, plus a pgvector index kept fresh from the same stream.
PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
DBT    ?= $(if $(wildcard .venv/bin/dbt),$(CURDIR)/.venv/bin/dbt,dbt)
export WAREHOUSE := $(CURDIR)/warehouse
OPS  ?= 50000
RATE ?= 1000
MODE ?= exactly-once

.PHONY: up down reset connector sink vector-sync workload dbt experiment experiment-naive experiment-faults search test

up:                      ## start Postgres, Kafka, Debezium; register the connector (snapshot starts)
	docker compose up -d --wait
	$(MAKE) --no-print-directory connector

connector:               ## create or update the Debezium connector (idempotent PUT)
	curl -sf -X PUT -H 'Content-Type: application/json' --data @connectors/source-postgres.json \
	  localhost:8083/connectors/ecommerce-source/config > /dev/null && echo "connector registered"

down:
	docker compose down

reset:                   ## wipe everything: containers, volumes, Kafka topics, Iceberg warehouse
	docker compose down -v
	rm -rf warehouse

sink:                    ## Kafka -> Iceberg changelog tables (Ctrl+C flushes and exits)
	$(PYTHON) -m cdc.sink

vector-sync:             ## products topic -> local bge-small embeddings -> pgvector
	$(PYTHON) -m cdc.vector_sync

workload:                ## OPS random inserts/updates/deletes at RATE ops/s
	$(PYTHON) -m cdc.workload --ops $(OPS) --rate $(RATE)

dbt:                     ## changelog -> deduped staging -> SCD2 history -> current state, with tests
	cd dbt && $(DBT) build --profiles-dir .

experiment: reset up     ## the measured run: workload + kill -9 of the sink mid-run + verification
	$(PYTHON) -m cdc.experiment --ops $(OPS) --rate $(RATE) --mode $(MODE)

experiment-naive:        ## same run with the v1 consumer design (Kafka auto-commit) as the baseline
	$(MAKE) --no-print-directory experiment MODE=naive

experiment-faults:       ## kill -9 Kafka Connect (Debezium) and the vector sync mid-run instead of the sink
	$(MAKE) --no-print-directory experiment MODE=faults

search:                  ## semantic product search over the CDC-fed index, e.g. make search Q="gift for a runner"
	$(PYTHON) -m cdc.vector_sync --search "$(Q)"

test:
	$(PYTHON) -m pytest -q
