# Projet Big Data — pipeline e-commerce

Chaine complete de traitement de donnees e-commerce, d'une source SQL
relationnelle jusqu'a un tableau de bord :

**Oracle (SQL, 3NF) → JSON denormalise → Cassandra → PySpark / Parquet → Elasticsearch → Kibana**

Le pipeline s'execute **une phase a la fois** : chaque brique est demarree,
utilisee, puis eteinte avant la suivante. Deux phases ne tournent jamais
ensemble, ce qui permet de faire tenir l'ensemble dans un GitHub Codespace de
16 Go.

## Prerequis

- Docker et Docker Compose v2
- Python 3.10 ou plus
- Java 17 (requis par PySpark en phase 3 uniquement)

## Demarrage

```bash
cp .env.example .env
make install
```

Puis, dans l'ordre :

```bash
make phase1 && make oracle-down    # Oracle    -> data/json/
make phase2                        # Cassandra <- data/json/
make phase3                        # Spark lit Cassandra -> data/parquet/
make cassandra-down                # Cassandra n'est plus utile
make phase4                        # Elastic   -> index + dashboard
```

Cassandra reste allume entre les phases 2 et 3 : c'est la seule phase qui lit
une base plutot qu'un fichier. Spark s'executant en local, sans conteneur, le
pic memoire reste maitrise.

`make help` liste toutes les cibles. Kibana est ensuite disponible sur
<http://localhost:5601>.

## Phases

| Phase | Conteneur | Entree | Sortie |
|-------|-----------|--------|--------|
| 1 — Source SQL | Oracle Free 23ai | — | `data/json/*.jsonl` |
| 2 — Denormalisation | Cassandra 5.0 | `data/json/` | keyspace `ecommerce` |
| 3 — Formatage | aucun (PySpark local) | Cassandra | `data/parquet/` |
| 4 — Indexation | Elasticsearch + Kibana | `data/parquet/` | index + dashboard |

Aucune phase ne se connecte a la precedente : le passage de temoin se fait par
des fichiers sur disque.

## Structure

```
sql/         schema Oracle, nettoyage, controles, requetes de denormalisation
cql/         modele Cassandra
pipeline/    code Python, un sous-paquet par phase
scripts/     un script bash par phase (demarrage, execution, rappel d'extinction)
kibana/      dashboard versionne, importe par script
docs/        justification des choix techniques, phase par phase
data/        artefacts intermediaires (ignore par git)
tests/       controles de coherence entre les phases
```

## Documentation

- [Architecture et execution sequentielle](docs/01-architecture.md)
- [Modele relationnel Oracle](docs/02-modele-oracle.md)
- [Modele Cassandra et denormalisation](docs/03-modele-cassandra.md)
