#!/usr/bin/env bash
# Fonctions communes aux scripts de phase.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Le code Python est importe en tant que package `pipeline` depuis la racine.
export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"

if [[ ! -f .env ]]; then
  echo "Fichier .env absent, creation depuis .env.example"
  cp .env.example .env
fi

titre() {
  printf '\n\033[1m== %s ==\033[0m\n' "$1"
}

# Attend qu'un conteneur passe a l'etat "healthy". On s'appuie sur le
# healthcheck declare dans docker-compose.yml plutot que sur une attente fixe :
# Oracle met de 40 s a plusieurs minutes a s'ouvrir selon la machine, et un
# `sleep` arbitraire serait soit trop court, soit du temps perdu.
attendre_healthy() {
  local conteneur="$1"
  local delai_max="${2:-600}"
  local ecoule=0

  echo "Attente du demarrage de $conteneur (delai max ${delai_max}s)"
  while true; do
    local etat
    etat="$(docker inspect -f '{{.State.Health.Status}}' "$conteneur" 2>/dev/null || echo "absent")"
    case "$etat" in
      healthy)
        echo "$conteneur est pret (${ecoule}s)"
        return 0
        ;;
      absent)
        echo "Le conteneur $conteneur n'existe pas." >&2
        return 1
        ;;
    esac
    if (( ecoule >= delai_max )); then
      echo "Delai depasse : $conteneur est reste dans l'etat '$etat'." >&2
      docker logs --tail 40 "$conteneur" >&2 || true
      return 1
    fi
    sleep 5
    ecoule=$(( ecoule + 5 ))
  done
}

# Rappel affiche en fin de phase : la phase suivante ne doit demarrer qu'apres
# extinction de la precedente.
rappel_extinction() {
  local profil="$1"
  cat <<EOF

Phase terminee. Eteignez cette phase avant de lancer la suivante :

    docker compose --profile ${profil} down

EOF
}
