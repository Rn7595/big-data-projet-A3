#!/usr/bin/env bash
# Phase 1 : source SQL Oracle -> documents JSON denormalises.
#
# Seul le conteneur Oracle est allume ici. En sortie, data/json/ contient les
# documents qui alimenteront Cassandra : Oracle peut alors etre eteint.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

titre "Demarrage d'Oracle"
docker compose --profile oracle up -d
attendre_healthy bde-oracle 900

titre "Creation du schema normalise"
python -m pipeline.phase1_oracle.init_schema

titre "Generation du jeu de donnees"
python -m pipeline.phase1_oracle.generate_data

titre "Nettoyage et controles de qualite"
python -m pipeline.phase1_oracle.data_quality

titre "Extraction JSON denormalisee"
python -m pipeline.phase1_oracle.extract_to_json

titre "Resultat"
ls -lh data/json/

rappel_extinction oracle
