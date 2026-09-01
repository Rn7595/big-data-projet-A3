# Demonstrations interactives pour la soutenance

Commandes a taper en direct, phase par phase, pour prouver le fonctionnement
plutot que de le raconter. Chacune est choisie pour **montrer un mecanisme**,
pas pour afficher des donnees.

## Contrainte a gerer : une phase a la fois

Une seule brique tourne a la fois. Deux facons de faire :

- **enregistrer par segments** : une phase, un segment, puis on eteint et on
  passe a la suivante. Le montage masque les attentes. C'est le plus propre ;
- **preparer les captures a l'avance** pour les phases eteintes, et ne faire
  vivre que la phase en cours.

Verifiez avant chaque segment :

```bash
make status
```

---

## Phase 1 — Oracle

```bash
docker compose --profile oracle up -d
docker exec -it bde-oracle sqlplus ecom/ecom@localhost/FREEPDB1
```

### Montrer que le schema est vraiment normalise

```sql
SET LINESIZE 200
SELECT table_name, num_rows FROM user_tables ORDER BY table_name;
```

```sql
-- Les cles etrangeres declarees : la coherence est portee par le SGBD
SELECT constraint_name, table_name FROM user_constraints
WHERE constraint_type = 'R' ORDER BY table_name;
```

> « Dix cles etrangeres. C'est le SGBD qui garantit qu'une ligne de commande
> ne peut pas referencer un produit inexistant. »

### La demonstration la plus parlante : le total absent

```sql
SELECT column_name FROM user_tab_columns WHERE table_name = 'ORDERS';
```

> « Aucune colonne de montant total. Il faut le calculer. »

```sql
SELECT o.order_id,
       SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct/100)) AS total
FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
WHERE o.order_id = 46463
GROUP BY o.order_id;
```

> « Une jointure et une agregation, a chaque lecture. Retenez ce chiffre : on
> va le retrouver deja calcule dans Cassandra. »

### Le controle qu'aucune contrainte ne peut porter

```sql
SELECT COUNT(*) FROM orders o
JOIN addresses a ON a.address_id = o.shipping_address_id
WHERE a.customer_id <> o.customer_id;
```

> « Zero. La cle etrangere garantit que l'adresse existe, pas qu'elle appartient
> au bon client : c'est mon controle de qualite qui le verifie. »

Quitter : `exit`, puis `docker compose --profile oracle down`.

---

## Phase 2 — Cassandra

```bash
docker compose --profile cassandra up -d
docker exec -it bde-cassandra cqlsh
```

### Le modele

```sql
DESCRIBE KEYSPACE ecommerce;
```

Ou, plus lisible a l'ecran :

```sql
DESCRIBE TABLE ecommerce.sales_by_category_month;
```

> « Regardez la cle primaire : la partition, c'est le couple categorie-mois. »

### LA demonstration a ne pas rater

```sql
USE ecommerce;

-- Avec la cle de partition : immediat
SELECT order_ref, order_date, total_amount
FROM orders_by_customer WHERE customer_id = 4317 LIMIT 5;
```

Puis, **la meme table sans la cle de partition** :

```sql
SELECT order_ref FROM orders_by_customer WHERE order_status = 'DELIVERED' LIMIT 5;
```

Cassandra **refuse** :

```
InvalidRequest: ... Cannot execute this query as it might involve data
filtering and thus may have unpredictable performance. If you want to execute
this query despite the performance unpredictability, use ALLOW FILTERING
```

> « Voila la difference avec SQL, en une erreur. Cassandra **refuse** une requete
> qu'il ne sait pas servir efficacement. Il ne la fait pas lentement : il la
> refuse. C'est pour ca que le modele part des requetes. »

Puis, pour enfoncer le clou :

```sql
SELECT order_ref FROM orders_by_customer WHERE order_status = 'DELIVERED'
LIMIT 5 ALLOW FILTERING;
```

> « Avec `ALLOW FILTERING` ca passe — en balayant toutes les partitions du
> cluster. C'est exactement ce que mon modele evite. »

### Prouver qu'une lecture ne touche qu'une partition

```sql
TRACING ON;
SELECT order_ref, total_amount FROM orders_by_customer WHERE customer_id = 4317 LIMIT 5;
TRACING OFF;
```

La trace montre le nombre de partitions lues et le temps en microsecondes.

### Le total pre-calcule

```sql
SELECT order_ref, items_count, total_amount FROM order_by_id WHERE order_id = 46463;
```

> « Le meme total qu'Oracle calculait par une jointure. Ici il est stocke : il a
> ete calcule une fois, a l'ecriture. »

```sql
SELECT items FROM order_by_id WHERE order_id = 46463;
```

> « Et les lignes de commande sont **dans** la ligne. Aucune jointure possible,
> aucune jointure necessaire. »

Quitter : `exit`.

---

## Phase 3 — Spark et Parquet

Cassandra doit rester allume seulement si vous relisez la base ; pour ces
demonstrations, Parquet suffit.

```bash
pyspark --driver-memory 2g
```

### Le partitionnement, montre par le plan d'execution

```python
df = spark.read.parquet("data/parquet/fact_order_items")
df.count()

novembre = df.filter("order_year = 2025 AND order_month = 11")
novembre.explain()
```

Cherchez `PartitionFilters` dans le plan affiche.

> « Spark ne lit pas les 24 repertoires : il n'ouvre que celui de novembre 2025.
> Le filtre est applique **avant** la lecture, pas apres. C'est le partition
> pruning. »

### Le colonnaire, montre par le schema

```python
df.printSchema()
df.select("net_amount").summary().show()
```

> « Une requete sur cette seule colonne ne lit que cette colonne sur le disque.
> En CSV il faudrait parcourir chaque ligne entiere. »

### Une analyse en direct

```python
from pyspark.sql import functions as F
(df.filter("is_revenue")
   .groupBy("parent_category_name")
   .agg(F.sum("net_amount").alias("ca"))
   .orderBy(F.desc("ca")).show(truncate=False))
```

Quitter : `exit()`.

### La comparaison de taille, hors Spark

```bash
du -sh data/json/orders.jsonl data/parquet/fact_order_items
```

---

## Phase 4 — Elasticsearch et Kibana

```bash
docker compose --profile elastic up -d
```

### Le mapping, en direct

```bash
curl -s localhost:9200/ecom-order-items/_mapping | python3 -m json.tool | head -40
```

### LA demonstration a ne pas rater

Agreger sur un champ `text` :

```bash
curl -s 'localhost:9200/ecom-order-items/_search?size=0' \
  -H 'Content-Type: application/json' \
  -d '{"aggs":{"produits":{"terms":{"field":"product_name"}}}}' | python3 -m json.tool | head -20
```

Elasticsearch **refuse** :

```
Text fields are not optimised for operations that require per-document
field data like aggregations and sorting...
```

Puis la meme chose sur le sous-champ `keyword` :

```bash
curl -s 'localhost:9200/ecom-order-items/_search?size=0' \
  -H 'Content-Type: application/json' \
  -d '{"aggs":{"produits":{"terms":{"field":"product_name.keyword","size":5}}}}' \
  | python3 -m json.tool | head -25
```

> « Voila pourquoi le mapping explicite compte. En mapping dynamique, tous mes
> libelles auraient ete analyses, et aucun regroupement n'aurait fonctionne. »

### Une agregation complete, avec son temps de reponse

```bash
curl -s 'localhost:9200/ecom-order-items/_search?size=0' \
  -H 'Content-Type: application/json' \
  -d '{"aggs":{"par_rayon":{"terms":{"field":"parent_category_name","order":{"ca":"desc"}},
       "aggs":{"ca":{"sum":{"field":"net_amount"}}}}}}' | python3 -m json.tool | head -30
```

> « Le champ `took` : quelques millisecondes pour agreger 149 300 documents. »

### Kibana, en interaction

Le plus convaincant visuellement :

1. cliquer sur un secteur du camembert d'un rayon — **tous** les panneaux se
   filtrent instantanement ;
2. changer la periode en haut a droite — la courbe se recalcule ;
3. taper dans la barre de recherche : `brand : "Nexora"` puis Entree.

> « Aucun de ces croisements n'etait prevu a l'avance. C'est ce que permet
> l'indexation au grain le plus fin. »

---

## La cloture : la preuve

```bash
make test
```

> « Sept controles, execution automatique. 149 300 lignes de commande dans
> Oracle, dans Cassandra, dans Parquet et dans Elasticsearch. Et le chiffre
> d'affaires calcule par Spark egale celui calcule par Elasticsearch, par deux
> chemins independants. »

---

## Repetez avant d'enregistrer

Tapez chaque commande une fois a blanc. Deux raisons : verifier qu'elle passe
sur votre jeu de donnees, et reperer les identifiants a utiliser (le
`customer_id = 4317` et la commande `46463` de ces exemples n'existeront pas apres un
nouveau chargement).

Pour retrouver un identifiant valide :

```bash
python -m pipeline.phase2_cassandra.demo_queries | head -20
```
