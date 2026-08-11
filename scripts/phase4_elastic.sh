#!/usr/bin/env bash
# Phase 4 : Parquet -> Elasticsearch -> tableau de bord Kibana.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if [[ ! -d data/parquet/fact_order_items ]]; then
  echo "data/parquet/fact_order_items est absent : lancez d'abord la phase 3." >&2
  exit 1
fi

# Elasticsearch refuse de demarrer si le noyau hote limite le nombre de zones
# memoire projetables. Le defaut d'un Codespace est tres en dessous du minimum
# exige, et l'echec se produit APRES le demarrage du conteneur : sans ce
# reglage, on ne voit qu'un conteneur qui s'arrete seul.
titre "Reglage du noyau pour Elasticsearch"
requis=262144
actuel="$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)"
if (( actuel < requis )); then
  echo "vm.max_map_count = $actuel, insuffisant (minimum $requis)"
  if sudo sysctl -w vm.max_map_count="$requis"; then
    echo "Reglage applique pour cette session."
  else
    echo "Impossible de modifier vm.max_map_count. Lancez manuellement :" >&2
    echo "    sudo sysctl -w vm.max_map_count=$requis" >&2
    exit 1
  fi
else
  echo "vm.max_map_count = $actuel, suffisant."
fi

titre "Demarrage d'Elasticsearch et de Kibana"
docker compose --profile elastic up -d
attendre_healthy bde-elasticsearch 300
attendre_healthy bde-kibana 600

titre "Indexation des fichiers Parquet"
python -m pipeline.phase4_elastic.index_parquet

titre "Vues de donnees et tableau de bord"
python -m pipeline.phase4_elastic.dashboard build

cat <<EOF

Pipeline complet.

Kibana         : ${KIBANA_URL:-http://localhost:5601}
Tableau de bord: ${KIBANA_URL:-http://localhost:5601}/app/dashboards

Dans un Codespace, ouvrez l'onglet "PORTS" de VS Code et cliquez sur l'adresse
du port 5601 ; l'adresse localhost ne fonctionne que depuis la machine distante.

Pour eteindre :

    docker compose --profile elastic down

EOF
