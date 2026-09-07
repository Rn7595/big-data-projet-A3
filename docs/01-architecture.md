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

Les phases 1 et 2 utilisent chacune un conteneur ; la phase 4 en utilise deux,
Elasticsearch et Kibana. La phase 3 execute Spark en local et conserve
Cassandra actif le temps de sa lecture.

## Pourquoi l'execution peut etre sequentielle

Convertir une base Oracle en base Cassandra sans que les deux tournent
simultanement est possible parce qu'**elles ne se connectent jamais l'une a
l'autre** : le passage de temoin entre ces deux phases est un fichier sur
disque.

Cette propriete ne vaut pas pour toutes les etapes. Le tableau ci-dessous
precise, pour chacune, la nature de l'entree.

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

| Phase | Conteneurs | `mem_limit` declare | Ordre de grandeur attendu |
|-------|-----------|---------------------|---------------------------|
| 1 | Oracle | 4 Go | 2 a 3 Go |
| 2 | Cassandra | 3 Go | ~2 Go (heap bornee a 1 Go) |
| 3 | Cassandra + Spark local | 3 Go + 2 Go | 4 a 5 Go |
| 4 | Elasticsearch + Kibana | 2,5 + 2 Go | 3 a 4 Go |

Les `mem_limit` sont des plafonds imposes aux conteneurs ; la derniere colonne
donne l'ordre de grandeur attendu, a confirmer par `docker stats` pendant le
premier run.

Les limites des quatre conteneurs totalisent 12 Go (4 + 3 + 2,5 + 2), auxquels
s'ajouteraient Spark, Python et le systeme. L'execution sequentielle reduit la
pression memoire. Les 4 a 5 Go annonces pour la phase 3 sont une estimation,
pas un plafond global garanti : le heap Spark de 2 Go ne couvre pas toute la
memoire du processus.

## Ce que les profils Compose apportent

Les services portent tous un `profiles:`. Consequence : `docker compose up`
sans argument ne demarre **rien**. Il faut nommer la phase.

```bash
docker compose --profile oracle up -d       # phase 1
docker compose --profile oracle down        # extinction
docker compose --profile cassandra up -d    # phase 2
```

Les profils separent les groupes de services et reduisent le risque d'un
demarrage involontaire. Ils n'interdisent pas d'activer plusieurs profils,
ensemble ou successivement, sans arreter les precedents.

La sequentialite complete repose donc sur trois elements complementaires :

| Element | Ce qu'il garantit |
|---|---|
| Les profils Compose | selection explicite des groupes de services |
| Les gardes des scripts de phase | phase 2 refuse Oracle actif ; phase 4 refuse Oracle ou Cassandra actifs |
| Les commandes d'arret entre les etapes | l'extinction effective, rappelee en fin de chaque script |

Les volumes sont nommes (`oracle_data`, `cassandra_data`, `es_data`,
`kibana_data`) : eteindre une phase n'efface pas ses donnees. On peut revenir
sur la phase 1 sans rejouer la generation.

## Enchainement complet

```bash
make install
make phase1 && make oracle-down
make phase2
make phase3
make cassandra-down
make phase4
```

Cassandra reste allume entre les phases 2 et 3 : Spark lit ses tables en direct.
La phase 2 verifie la presence du JSON et l'arret d'Oracle ; la phase 3 attend
Cassandra ; la phase 4 verifie la presence du Parquet et l'arret d'Oracle et de
Cassandra. L'utilisateur lance les commandes d'arret indiquees entre les phases.

Chaque script de phase attend l'etat `healthy` du conteneur (defini par les
`healthcheck` du Compose) plutot qu'une temporisation fixe, et rappelle en fin
d'execution la commande d'extinction a lancer.
