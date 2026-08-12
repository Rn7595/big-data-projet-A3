# Deroule operatoire de l'enregistrement

Ce document dit **quoi taper et quand**. Pour ce qu'il faut dire, voir
`docs/06-script-video.md`. Pour le detail des demonstrations, voir
`docs/07-demonstration-live.md`.

Principe : **un segment par phase, un conteneur a la fois.** On enregistre six
segments courts, on les assemble au montage. Un segment rate se refait seul.

---

## Preparation (15 minutes, avant d'enregistrer)

Rouvrir le Codespace, puis :

```bash
cd /workspaces/big-data-projet-A3
git status --short          # doit etre vide (hors logs-*.txt)
make status                 # doit etre vide : rien ne tourne
ls -lh data/json data/parquet data/reports
```

Les trois repertoires de `data/` doivent exister et etre remplis. S'ils sont
vides, le pipeline doit etre rejoue en entier avant d'enregistrer.

**Reglages d'affichage :**

- police du terminal : `Ctrl` + `+` deux ou trois fois ;
- explorateur de fichiers ferme : `Ctrl+B` ;
- panneau de terminal agrandi : faire glisser sa bordure vers le haut ;
- onglets a ouvrir d'avance : `docker-compose.yml`, `sql/01_schema.sql`,
  `sql/10_extract_orders.sql`, `cql/02_tables.cql`,
  `pipeline/phase3_spark/transforms.py`,
  `pipeline/phase4_elastic/mappings/order_items.json`.

**Test d'enregistrement de 30 secondes**, puis relecture : le texte du terminal
doit rester lisible apres compression. C'est la verification qui evite de
refaire les dix minutes.

---

## Segment A — Introduction et architecture

**Conteneur : aucun.**

```bash
make status
```

> « Rien ne tourne. »

Montrer `docker-compose.yml`, pointer les lignes `profiles:`.

```bash
docker compose up -d
```

Aucun conteneur ne demarre : sans profil nomme, Compose n'a rien a lancer.

> « La sequentialite n'est pas une regle que je dois me rappeler : le fichier
> rend impossible de tout allumer par megarde. »

---

## Segment B — Phase 1, Oracle

**Conteneur : Oracle seul.**

```bash
docker compose --profile oracle up -d
docker compose logs -f oracle | tail -5
```

Le demarrage prend 60 a 90 secondes. **A couper au montage.**

```bash
docker exec -it bde-oracle sqlplus ecom/ecom@localhost/FREEPDB1
```

Dans SQL*Plus :

```sql
SET LINESIZE 200
SET PAGESIZE 50

-- Le schema
SELECT table_name FROM user_tables ORDER BY table_name;

-- Les cles etrangeres : la coherence portee par le SGBD
SELECT COUNT(*) AS nb_cles_etrangeres FROM user_constraints WHERE constraint_type = 'R';

-- Ce qui n'est PAS dans ORDERS
SELECT column_name FROM user_tab_columns WHERE table_name = 'ORDERS';

-- Donc il faut calculer le total
SELECT o.order_id,
       ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct/100)), 2) AS total
FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
WHERE o.order_id = 35704
GROUP BY o.order_id;

-- Le controle qu'aucune contrainte ne peut porter
SELECT COUNT(*) AS adresses_incoherentes
FROM orders o JOIN addresses a ON a.address_id = o.shipping_address_id
WHERE a.customer_id <> o.customer_id;

exit
```

Puis le livrable de la phase :

```bash
head -1 data/json/orders.jsonl | python3 -m json.tool | head -40
```

Extinction :

```bash
docker compose --profile oracle down
```

---

## Segment C — Phase 2, Cassandra

**Conteneur : Cassandra seul.**

```bash
docker compose --profile cassandra up -d
```

Attendre 60 a 90 secondes, **a couper au montage**. Verifier :

```bash
docker exec bde-cassandra nodetool status
```

Puis :

```bash
docker exec -it bde-cassandra cqlsh
```

Dans cqlsh :

```sql
USE ecommerce;

-- Le modele
DESCRIBE TABLE sales_by_category_month;

-- Avec la cle de partition : immediat
SELECT order_ref, order_date, order_status, total_amount
FROM orders_by_customer WHERE customer_id = 4317 LIMIT 5;

-- SANS la cle de partition : Cassandra refuse
SELECT order_ref FROM orders_by_customer WHERE order_status = 'DELIVERED' LIMIT 5;

-- Avec ALLOW FILTERING : ca passe, en balayant tout
SELECT order_ref FROM orders_by_customer WHERE order_status = 'DELIVERED'
LIMIT 5 ALLOW FILTERING;

-- Le total pre-calcule, et les lignes imbriquees
SELECT order_ref, items_count, total_amount FROM order_by_id WHERE order_id = 35704;
SELECT items FROM order_by_id WHERE order_id = 35704;

exit
```

**Le refus de la troisieme requete est le meilleur moment de la video.** Ne le
coupez pas au montage.

Puis les quatre requetes du modele :

```bash
python -m pipeline.phase2_cassandra.demo_queries
```

Extinction :

```bash
docker compose --profile cassandra down
```

---

## Segment D — Phase 3, Spark et Parquet

**Conteneur : aucun.** Les demonstrations portent sur les fichiers Parquet
deja ecrits.

```bash
ls data/parquet/fact_order_items | head
ls data/parquet/fact_order_items/order_year=2025/
du -sh data/json/orders.jsonl data/parquet/fact_order_items
```

> « 76 Mo de JSON, 5 Mo de Parquet. »

Puis, dans PySpark :

```bash
export JAVA_HOME=$(ls -d /usr/local/sdkman/candidates/java/21* 2>/dev/null | head -1)
export PATH="$JAVA_HOME/bin:$PATH"
pyspark --driver-memory 2g
```

```python
df = spark.read.parquet("data/parquet/fact_order_items")
df.count()

# Le partitionnement, vu par le plan d'execution
df.filter("order_year = 2025 AND order_month = 11").explain()

# Une analyse transverse, impossible en Cassandra
from pyspark.sql import functions as F
(df.filter("is_revenue")
   .groupBy("parent_category_name")
   .agg(F.sum("net_amount").alias("ca"))
   .orderBy(F.desc("ca")).show(truncate=False))

exit()
```

Dans la sortie de `explain()`, pointer la ligne `PartitionFilters`.

---

## Segment E — Phase 4, Elasticsearch et Kibana

**Conteneurs : Elasticsearch et Kibana.**

```bash
docker compose --profile elastic up -d
```

Attendre environ 90 secondes, **a couper au montage**.

```bash
curl -s localhost:9200/_cluster/health | python3 -m json.tool
curl -s localhost:9200/_cat/indices/ecom-*?v
```

La demonstration du mapping :

```bash
# Agreger sur un champ analyse : refuse
curl -s 'localhost:9200/ecom-order-items/_search?size=0' \
  -H 'Content-Type: application/json' \
  -d '{"aggs":{"produits":{"terms":{"field":"product_name"}}}}' \
  | python3 -m json.tool | head -15

# Sur le sous-champ keyword : ca marche
curl -s 'localhost:9200/ecom-order-items/_search?size=0' \
  -H 'Content-Type: application/json' \
  -d '{"aggs":{"produits":{"terms":{"field":"product_name.keyword","size":5}}}}' \
  | python3 -m json.tool | head -30
```

Une agregation complete, avec son temps de reponse :

```bash
curl -s 'localhost:9200/ecom-order-items/_search?size=0' \
  -H 'Content-Type: application/json' \
  -d '{"aggs":{"par_rayon":{"terms":{"field":"parent_category_name","order":{"ca":"desc"}},
       "aggs":{"ca":{"sum":{"field":"net_amount"}}}}}}' | python3 -m json.tool | head -30
```

Pointer le champ `took` : quelques millisecondes.

**Kibana**, onglet PORTS de VS Code, port 5601, icone du globe. Puis :

1. menu, **Dashboards**, ouvrir le tableau de bord ;
2. cliquer sur un secteur du camembert : tous les panneaux se filtrent ;
3. retirer le filtre, taper dans la barre : `brand : "Nexora"` puis Entree ;
4. changer la periode en haut a droite.

---

## Segment F — La preuve, et la conclusion

**Conteneur : aucun.** Les controles lisent les rapports sur disque, pas les
bases : ils tournent tout eteint.

```bash
docker compose --profile elastic down
make status
make test
```

> « Sept controles. La donnee traverse la chaine sans perte, et deux moteurs
> independants trouvent le meme chiffre d'affaires. »

C'est la derniere image de la video.

---

## Apres l'enregistrement

Le zip a rendre :

```bash
cd ..
zip -r projet-bigdata-ecommerce.zip big-data-projet-A3 \
  -x '*/.git/*' '*/data/*' '*/__pycache__/*' '*/logs-*.txt'
ls -lh projet-bigdata-ecommerce.zip
```

Le telecharger : clic droit sur le fichier dans l'explorateur VS Code,
**Download**.

Puis liberer les ressources :

```bash
cd big-data-projet-A3
make down
```

Et arreter le Codespace depuis <https://github.com/codespaces>.

**Ne supprimez le Codespace qu'apres avoir rendu**, et apres avoir verifie que
le zip telecharge s'ouvre correctement.

---

## Aide-memoire des temps de demarrage

| Conteneur | Delai avant utilisation |
|---|---|
| Oracle | 60 a 90 s |
| Cassandra | 60 a 90 s, plus 10 s avant que le port CQL accepte |
| Elasticsearch | 30 a 40 s |
| Kibana | 40 a 90 s apres Elasticsearch |

Ces attentes se coupent au montage. Lancez le conteneur, mettez
l'enregistrement en pause, reprenez quand il est pret.
