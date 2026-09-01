#!/usr/bin/env bash
# Phase 3 : lecture Cassandra par Spark -> Parquet.
#
# Aucun conteneur Spark : PySpark tourne en local, dans le processus Python.
# Cassandra doit en revanche etre allume, c'est la seule phase qui lit une base
# plutot qu'un fichier.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# --- Selection d'un JDK compatible ------------------------------------------
#
# Spark 3.5 est officiellement supporte sur Java 8, 11 et 17. Java 21 a ete
# verifie comme fonctionnel sur ce projet, il est donc accepte en repli. Au-dela,
# les JVM verrouillent l'acces reflexif a leurs internes, dont Spark depend pour
# sa serialisation : l'echec se manifeste par des InaccessibleObjectException
# illisibles, tres loin de la cause reelle.
#
# On cherche donc un JDK compatible parmi les emplacements habituels avant de
# lancer quoi que ce soit, en preferant la version supportee officiellement.

version_majeure() {
  "$1/bin/java" -version 2>&1 | head -1 | grep -oE '[0-9]+' | head -1
}

trouver_jdk() {
  local recherchee="$1" base entree
  for base in /usr/lib/jvm "$HOME/.sdkman/candidates/java" \
              /usr/local/sdkman/candidates/java /opt/java /opt/jdk; do
    [[ -d "$base" ]] || continue
    for entree in "$base"/*; do
      [[ -x "$entree/bin/java" ]] || continue
      if [[ "$(version_majeure "$entree")" == "$recherchee" ]]; then
        echo "$entree"
        return 0
      fi
    done
  done
  return 1
}

titre "Selection du JDK"
version_courante="$(java -version 2>&1 | head -1 | grep -oE '[0-9]+' | head -1)"

if [[ "$version_courante" =~ ^(8|11|17|21)$ ]]; then
  echo "Java $version_courante convient, aucun changement necessaire."
else
  echo "Java $version_courante est incompatible avec Spark 3.5 (attendu : 8, 11, 17 ou 21)."
  jdk=""
  for candidate in 17 21 11; do
    if jdk="$(trouver_jdk "$candidate")"; then
      break
    fi
    jdk=""
  done

  if [[ -z "$jdk" ]]; then
    cat <<'EOF' >&2

Aucun JDK compatible n'a ete trouve sur cette machine.

Installez Java 17, puis relancez la phase :

    sudo apt-get update && sudo apt-get install -y openjdk-17-jdk

Ou, si SDKMAN est disponible :

    sdk install java 17.0.13-ms

EOF
    exit 1
  fi

  export JAVA_HOME="$jdk"
  export PATH="$JAVA_HOME/bin:$PATH"
  echo "JAVA_HOME force sur $JAVA_HOME"
  java -version
fi

# --- Verifications de phase --------------------------------------------------
if ! docker ps --format '{{.Names}}' | grep -q '^bde-cassandra$'; then
  echo "Cassandra n'est pas demarre. La phase 3 lit ses tables :" >&2
  echo "    docker compose --profile cassandra up -d" >&2
  exit 1
fi

# Le conteneur peut tourner sans accepter encore de connexion : Cassandra met
# une minute a ouvrir son port CQL, et davantage apres une mise en veille du
# poste. Sans cette attente, Spark echoue sur une erreur de connexion alors que
# la base allait etre prete quelques secondes plus tard.
attendre_healthy bde-cassandra 300

titre "Conversion Cassandra -> Parquet"
python -m pipeline.phase3_spark.build_parquet

titre "Resultat"
du -sh data/parquet/* 2>/dev/null || true

cat <<'EOF'

Phase terminee. Cassandra n'est plus necessaire :

    docker compose --profile cassandra down
    make phase4

EOF
