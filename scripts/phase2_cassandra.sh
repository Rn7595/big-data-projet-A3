#!/usr/bin/env bash
# Phase 2 : documents JSON denormalises -> Cassandra.
#
# Oracle doit etre eteint avant de lancer ce script. Ce n'est pas une simple
# precaution memoire : ce chargeur ne connait aucune connexion Oracle, il ne lit
# que data/json/.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if [[ ! -f data/json/orders.jsonl ]]; then
  echo "data/json/orders.jsonl est absent : lancez d'abord la phase 1." >&2
  exit 1
fi

if docker ps --format '{{.Names}}' | grep -q '^bde-oracle$'; then
  echo "Le conteneur Oracle tourne encore. Eteignez-le avant la phase 2 :" >&2
  echo "    docker compose --profile oracle down" >&2
  exit 1
fi

titre "Demarrage de Cassandra"
docker compose --profile cassandra up -d
attendre_healthy bde-cassandra 600

titre "Creation du modele CQL"
python -m pipeline.phase2_cassandra.create_schema

titre "Chargement des documents JSON"
python -m pipeline.phase2_cassandra.load_json

titre "Les quatre requetes du modele"
python -m pipeline.phase2_cassandra.demo_queries

cat <<'EOF'

Phase terminee. Cassandra reste allume : la phase 3 lit ces tables avec Spark.
C'est le script de la phase 3 qui l'eteindra une fois le Parquet ecrit.

    make phase3

EOF
