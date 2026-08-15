SHELL := /bin/bash
COMPOSE := docker compose

.DEFAULT_GOAL := help

.PHONY: help install phase1 phase2 phase3 phase4 test tracer \
        oracle-up oracle-down cassandra-up cassandra-down elastic-up elastic-down \
        down status clean-data reset

help:
	@echo "Pipeline Big Data e-commerce -- execution sequentielle, une phase a la fois"
	@echo
	@echo "  make install         installe les dependances Python"
	@echo "  make phase1          Oracle    : schema, donnees, nettoyage, extraction JSON"
	@echo "  make phase2          Cassandra : creation du modele et chargement du JSON"
	@echo "  make phase3          Spark     : conversion en Parquet et analyses (local)"
	@echo "  make phase4          Elastic   : indexation et dashboard Kibana"
	@echo
	@echo "  make oracle-down     eteint la phase 1 (a faire avant la phase 2)"
	@echo "  make cassandra-down  eteint la phase 2"
	@echo "  make elastic-down    eteint la phase 4"
	@echo "  make down            eteint tout ce qui traine"
	@echo "  make test            controles de coherence entre les phases"
	@echo "  make tracer CMD=35704  suit une commande a travers toutes les sources"
	@echo "  make status          affiche les conteneurs en cours"
	@echo "  make clean-data      supprime les artefacts de data/"
	@echo "  make reset           supprime aussi les volumes Docker"

install:
	pip install -r requirements.txt

phase1:
	./scripts/phase1_oracle.sh

phase2:
	./scripts/phase2_cassandra.sh

phase3:
	./scripts/phase3_spark.sh

phase4:
	./scripts/phase4_elastic.sh

oracle-up:
	$(COMPOSE) --profile oracle up -d

oracle-down:
	$(COMPOSE) --profile oracle down

cassandra-up:
	$(COMPOSE) --profile cassandra up -d

cassandra-down:
	$(COMPOSE) --profile cassandra down

elastic-up:
	$(COMPOSE) --profile elastic up -d

elastic-down:
	$(COMPOSE) --profile elastic down

down:
	$(COMPOSE) --profile oracle --profile cassandra --profile elastic down

status:
	@docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

clean-data:
	rm -rf data/json data/parquet data/reports

# Detruit les volumes : la prochaine phase 1 repartira d'une base vide.
reset: down clean-data
	$(COMPOSE) --profile oracle --profile cassandra --profile elastic down -v

test:
	python -m tests.test_coherence_pipeline

# Trace une commande a travers toutes les sources disponibles :
#   make tracer CMD=35704
tracer:
	python -m tests.tracer_commande $(CMD)
