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
- Java 17 ou 21 (requis par PySpark en phase 3 uniquement)

Le script de la phase 3 detecte la version de Java et bascule automatiquement
sur un JDK compatible s'il en trouve un.

Si `pip install pyspark` echoue sur `AttributeError: install_layout`, c'est une
incompatibilite entre les versions recentes de setuptools et le `setup.py` de
PySpark 3.5. Contournement :

```bash
pip install "setuptools<75" wheel
pip install --no-build-isolation pyspark==3.5.3
```

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

`make test` verifie la coherence des volumetries entre les quatre phases.
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

## Conformite au sujet

| Etape demandee | Ou elle est traitee | Resultat concret |
|---|---|---|
| **1.** Theme au choix, source **SQL sous Oracle**, structure **normalisee** | `sql/01_schema.sql`, `sql/02_indexes.sql` | 8 tables en 3NF, 10 cles etrangeres, 40 categories, 800 produits, 5 000 clients, 60 000 commandes, 149 186 lignes |
| Nettoyage des donnees (synopsis) | `sql/20_nettoyage.sql`, `sql/21_controles.sql`, `pipeline/phase1_oracle/data_quality.py` | 5 regles de normalisation, 8 controles dont 6 bloquants |
| **2.** Denormaliser et extraire vers Cassandra **via un fichier JSON** | `sql/10_extract_orders.sql`, `pipeline/phase2_cassandra/` | `data/json/orders.jsonl` (59 667 documents, 76 Mo) puis 4 tables Cassandra |
| **3.** Formater pour **Spark ou Parquet**, avec des fonctions Python d'analyse | `pipeline/phase3_spark/transforms.py`, `build_parquet.py` | `data/parquet/` : table de faits partitionnee + 3 agregats + segmentation RFM, 5 Mo |
| **4.** Indexer dans **Elasticsearch**, exposer dans **Kibana** | `pipeline/phase4_elastic/`, `kibana/dashboard.ndjson` | 2 index (149 186 + 3 457 documents), tableau de bord de 8 panneaux |
| Livrable : video de 10 minutes | `docs/06-script-video.md`, `docs/08-deroule-enregistrement.md` | deroule minute par minute |
| Livrable : archive du code | commande de zip dans `docs/08` | archive sans `.git` ni donnees |

Deux precisions sur les choix laisses libres par le sujet :

- le sujet autorise « Parquet **ou** Spark » : les deux sont utilises, Spark
  produisant le Parquet ;
- pandas est cite comme une option (« you can also apply python packages like
  pandas »). Il n'est pas utilise ici : la normalisation est faite en SQL a
  l'etape 1, les transformations par PySpark a l'etape 3, et la relecture du
  Parquet par PyArrow, qui lit par lots sans materialiser 149 000 lignes en
  memoire. Charger le tout dans un DataFrame pandas aurait ete un choix
  contraire a l'esprit du sujet.

## Documentation

- [Architecture et execution sequentielle](docs/01-architecture.md)
- [Modele relationnel Oracle](docs/02-modele-oracle.md)
- [Modele Cassandra et denormalisation](docs/03-modele-cassandra.md)
- [Formatage Spark et Parquet](docs/04-spark-parquet.md)
- [Indexation Elasticsearch et dashboard Kibana](docs/05-elasticsearch-kibana.md)
- [Script de la soutenance video](docs/06-script-video.md)
- [Demonstrations interactives](docs/07-demonstration-live.md)
- [Deroule operatoire de l enregistrement](docs/08-deroule-enregistrement.md)
