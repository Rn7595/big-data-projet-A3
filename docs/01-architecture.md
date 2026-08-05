# Architecture du pipeline

## Vue d'ensemble

```
   PHASE 1              PHASE 2              PHASE 3              PHASE 4
 ┌──────────┐        ┌───────────┐        ┌──────────┐       ┌───────────────┐
 │  Oracle  │        │ Cassandra │        │  Spark   │       │ Elasticsearch │
 │   23ai   │        │    5.0    │        │  local   │       │      +        │
 │(3NF, SQL)│        │ (NoSQL)   │        │ (PySpark)│       │    Kibana     │
 └────┬─────┘        └─────┬─────┘        └────┬─────┘       └───────┬───────┘
      │                    │                   │                     │
      │ JSON Lines         │ lecture CQL       │ Parquet             │ bulk API
      ▼                    ▼                   ▼                     ▼
  data/json/          tables denormalisees  data/parquet/       index + dashboard
```

Chaque phase est un conteneur unique (sauf la phase 3, sans conteneur), allume
puis eteint avant la suivante.

## Pourquoi l'execution peut etre sequentielle

C'est la question que le jury posera en premier : comment convertir une base
Oracle en base Cassandra si les deux ne tournent jamais ensemble ?

Parce qu'elles ne se parlent jamais. **Le passage de temoin entre deux phases
est un fichier sur disque, pas une connexion.**

| Phase | Conteneur allume | Entree | Sortie |
|-------|------------------|--------|--------|
| 1 | Oracle | — | `data/json/orders.jsonl`, `products.jsonl` |
| 2 | Cassandra | `data/json/` | tables du keyspace `ecommerce` |
| 3 | Cassandra (lecture) | Cassandra | `data/parquet/` |
| 4 | Elasticsearch + Kibana | `data/parquet/` | index ES + dashboard |

Oracle est eteint avant que Cassandra ne demarre : le chargeur de la phase 2
ne connait qu'un chemin de fichier. C'est aussi ce que demande le sujet, qui
impose de « produire un fichier JSON adapte a la structure NoSQL » — le JSON
n'est pas un detour, c'est le contrat d'interface entre les deux SGBD.

Seule exception assumee : la phase 3 lit Cassandra en direct via le connecteur
Spark officiel. Spark s'executant en local, sans conteneur, le pic memoire
reste de l'ordre de 5 Go (Cassandra 3 Go + JVM Spark 2 Go).

## Budget memoire

Les 16 Go d'un Codespace sont partages avec l'IDE et le systeme. Les plafonds
sont declares dans `docker-compose.yml` :

| Phase | Conteneurs | `mem_limit` | Pic reel observe |
|-------|-----------|-------------|------------------|
| 1 | Oracle | 4 Go | ~2,5 Go |
| 2 | Cassandra | 3 Go | ~1,8 Go (heap bornee a 1 Go) |
| 3 | Cassandra + Spark local | 3 Go + 2 Go | ~4,5 Go |
| 4 | Elasticsearch + Kibana | 2,5 + 2 Go | ~3,5 Go |

Si les quatre phases tournaient ensemble, le total declare atteindrait 11,5 Go,
auxquels s'ajouteraient les JVM locales : la machine tiendrait mal. En
sequentiel, on ne depasse jamais 5 Go.

## Ce que les profils Compose garantissent

Les services portent tous un `profiles:`. Consequence : `docker compose up`
sans argument ne demarre **rien**. Il faut nommer la phase.

```bash
docker compose --profile oracle up -d       # phase 1
docker compose --profile oracle down        # extinction
docker compose --profile cassandra up -d    # phase 2
```

La contrainte de sequentialite n'est donc pas une regle de conduite que
l'operateur doit se rappeler : elle est portee par le fichier de configuration.
Il n'existe aucune commande courte qui allume tout par inadvertance.

Les volumes sont nommes (`oracle_data`, `cassandra_data`, `es_data`,
`kibana_data`) : eteindre une phase n'efface pas ses donnees. On peut revenir
sur la phase 1 sans rejouer la generation.

## Enchainement complet

```bash
make install
make phase1 && make oracle-down
make phase2 && make cassandra-down
make phase3
make phase4
```

Chaque script de phase attend l'etat `healthy` du conteneur (defini par les
`healthcheck` du Compose) plutot qu'une temporisation fixe, et rappelle en fin
d'execution la commande d'extinction a lancer.
