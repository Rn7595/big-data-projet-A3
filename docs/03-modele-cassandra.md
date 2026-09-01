# Phase 2 — Modele Cassandra et denormalisation

Cette phase repond a une seule question : **comment passe-t-on d'un schema
concu pour eviter la redondance a un schema concu pour eviter les jointures ?**

## Le renversement de methode

| | Oracle | Cassandra |
|---|---|---|
| Point de depart | les entites du domaine | les requetes a servir |
| Regle | ne jamais dupliquer | dupliquer autant que necessaire |
| Jointure | le moteur la fait | elle n'existe pas |
| Tri | calcule a la lecture | ecrit sur le disque |
| Agregat | calcule a la lecture | calcule a l'ecriture |

En SQL, on modelise les entites, puis on ecrit n'importe quelle requete : le
moteur se debrouille. En Cassandra, **une table repond a une requete**. Si une
cinquieme requete apparait, on cree une cinquieme table.

## Comment fonctionne une cle primaire Cassandra

Ce fonctionnement conditionne les choix decrits ensuite.

```
PRIMARY KEY ( (cle_de_partition) , colonne_clustering_1, colonne_clustering_2 )
                    │                         │
                    │                         └─ ordre de tri SUR LE DISQUE,
                    │                            a l'interieur de la partition
                    └─ hachee → designe le noeud qui detient la donnee
```

Deux consequences pratiques :

- une lecture **doit** fournir la cle de partition complete, sinon Cassandra
  doit interroger tous les noeuds (*scatter-gather*) ;
- un tri obtenu par la cle de clustering **ne coute rien** : il est deja fait,
  les lignes sont physiquement rangees dans cet ordre.

## Les quatre requetes et leurs tables

Fichier : `cql/02_tables.cql`.

### Q1 — l'historique d'un client

```cql
PRIMARY KEY ((customer_id), order_date, order_id)
WITH CLUSTERING ORDER BY (order_date DESC, order_id DESC)
```

**Pourquoi `customer_id` en cle de partition :**

- *forte cardinalite* — 5 000 valeurs distinctes, donc une repartition
  uniforme sur l'anneau. Aucun point chaud ;
- *taille bornee* — meme un tres gros client depasse rarement quelques
  centaines de commandes. On reste tres loin des 100 Mo par partition
  au-dela desquels les performances se degradent ;
- *elle correspond a la question posee* — la requete connait toujours le
  client dont elle veut l'historique.

**Pourquoi `order_date DESC` en clustering :** la question est « les plus
recentes d'abord ». Avec ce tri ecrit sur le disque, « les 10 dernieres
commandes » lit les 10 premieres lignes de la partition et s'arrete. Aucun tri
a l'execution.

**Pourquoi `order_id` en plus :** une cle primaire doit etre unique. Deux
commandes passees a la meme seconde par le meme client auraient la meme cle,
et la seconde ecraserait silencieusement la premiere.

### Q2 — une commande par son identifiant

```cql
PRIMARY KEY (order_id)
```

Les **memes donnees** que Q1, dans une seconde table. C'est le principe « une
table par requete » applique jusqu'au bout.

Pourquoi ne pas interroger `orders_by_customer` ? Parce qu'une lecture exige la
cle de partition. Chercher une commande par son seul identifiant dans une table
partitionnee par client obligerait a balayer toutes les partitions.

Le cout est un doublement du volume et deux ecritures au lieu d'une. **C'est le
compromis explicite du modele** : le stockage est bon marche, la lecture
distribuee ne l'est pas.

### Q3 — le chiffre d'affaires par categorie et par mois

```cql
PRIMARY KEY ((category_id, year_month), order_date, order_id, line_no)
```

**La cle de partition est composite.**

Partitionner par la seule `category_id` donnerait 32 partitions qui
grossiraient indefiniment au fil des mois. Au bout de quelques annees, une
categorie populaire depasserait la limite pratique : c'est l'**anti-pattern de
la partition non bornee**, le plus frequent en modelisation Cassandra.

Ajouter le mois dans la cle de partition est un **bucketing temporel**. La
partition est bornee par construction : elle ne contient qu'un mois. Le nombre
de partitions croit avec le temps — ce qui est exactement le comportement
souhaitable d'un systeme distribue, ou la charge se repartit sur de nouveaux
noeuds.

Effet secondaire recherche : la requete metier porte justement sur un couple
(categorie, mois). Elle lit donc **une partition et une seule**.

**Granularite :** une ligne par ligne de commande, pas par commande. Une
commande touchant trois categories alimente trois partitions differentes.
C'est ce qui permet d'imputer chaque euro a sa categorie, et c'est cette table
que Spark lira en phase 3 comme table de faits.

### Q4 — le catalogue d'une categorie

```cql
PRIMARY KEY ((category_id), product_name, product_id)
```

Tri alphabetique obtenu par la cle de clustering, sans `ORDER BY`. La
hierarchie des categories — exprimee en SQL par une cle etrangere reflexive —
est aplatie en deux colonnes : CQL n'a pas d'equivalent de la requete
recursive.

## Les anti-patterns ecartes

Trois modelisations alternatives ont ete ecartees, pour les raisons
suivantes :

| Choix | Pourquoi c'est mauvais |
|---|---|
| Partitionner par `order_date` | toutes les commandes du jour tombent sur le meme noeud, qui encaisse tout le trafic d'ecriture. Point chaud. |
| Partitionner par `order_status` | six valeurs distinctes → six partitions enormes, anneau totalement desequilibre. |
| Index secondaire sur `customer_id` | la requete devient un scatter-gather interrogeant tous les noeuds. Un index secondaire Cassandra n'est pas un index SQL. |

## Le type utilisateur `order_item`

```cql
items list<frozen<order_item>>
```

C'est **la** denormalisation. La relation 1-N entre `ORDERS` et `ORDER_ITEMS`,
qui exigeait une table separee et une jointure en SQL, devient une collection
imbriquee dans la ligne de commande. Le produit, sa marque et sa categorie sont
recopies dedans : en SQL il aurait fallu deux jointures de plus pour les
obtenir.

**Pourquoi `frozen` :** Cassandra serialise alors la collection comme une
valeur unique et immuable. La consequence est a assumer — modifier une seule
ligne de commande impose de reecrire tout le tableau. C'est acceptable ici
parce qu'**une commande passee ne change plus**. Ce serait un mauvais choix
pour une donnee mise a jour element par element.

## Ce que l'on a perdu

Le modele a un cout, qu'il faut enoncer :

- **l'integrite referentielle** — plus aucune cle etrangere. Si un nom de
  produit change, les commandes passees gardent l'ancien. Ici c'est voulu
  (c'est de l'historisation), mais rien ne l'impose plus techniquement ;
- **les requetes imprevues** — toute question non anticipee exige une nouvelle
  table et un rechargement. En SQL, il aurait suffi d'ecrire une requete ;
- **l'unicite de la verite** — la meme commande existe dans deux tables. Une
  ecriture partielle les desynchronise, et rien ne le detectera ;
- **les agregats libres** — `SUM` n'est possible qu'a l'interieur d'une
  partition. C'est precisement pour cela que la phase 3 existe : Spark prend en
  charge les agregations transverses que Cassandra refuse.

Ce qu'on a gagne en echange : des lectures a une seule partition, un tri
gratuit, et une montee en charge horizontale.

## La replication

```cql
{'class': 'SimpleStrategy', 'replication_factor': 1}
```

Cluster mono-noeud de demonstration : il n'y a rien a repliquer, et toute autre
valeur produirait un keyspace incapable de satisfaire ses propres exigences de
coherence.

**En production**, on ecrirait `NetworkTopologyStrategy` avec RF = 3 par
datacenter, et des lectures comme des ecritures en `LOCAL_QUORUM`. Avec RF = 3,
QUORUM vaut 2 : la regle **R + W > RF** (2 + 2 > 3) garantit qu'une lecture
voit toujours la derniere ecriture, tout en tolerant la perte d'un noeud. Le
snitch `GossipingPropertyFileSnitch` deja configure dans `docker-compose.yml`
rendrait ce passage possible sans reconfiguration.

## Le chargement

Fichier : `pipeline/phase2_cassandra/load_json.py`.

**Le chargeur ne connait aucune connexion Oracle.** Il lit `data/json/`. C'est
ce qui rend l'execution sequentielle possible, et le script refuse d'ailleurs
de demarrer si le conteneur Oracle tourne encore.

**Pas de `BatchStatement`.** C'est un anti-pattern des qu'un lot touche
plusieurs partitions : le coordinateur devrait attendre tous les noeuds
concernes, et le lot deviendrait plus lent que les ecritures individuelles
qu'il pretend remplacer. Un batch Cassandra garantit l'atomicite dans une
partition, il n'est pas un outil de performance. On utilise
`execute_concurrent_with_args`, qui maintient 64 requetes en vol.

**Types monetaires en `decimal`, pas `double`.** Un `double` ne represente pas
exactement 19,90 et l'erreur s'accumulerait sur des centaines de milliers de
lignes agregees.

**Verification finale :** le script compte les lignes de chaque table et les
compare au nombre de lignes ecrites. Un `COUNT(*)` sans cle de partition est
justement le balayage que tout le modele cherche a eviter — c'est acceptable
pour un controle ponctuel, jamais en usage courant.

## Commandes

```bash
make phase2    # ou : ./scripts/phase2_cassandra.sh
```

Le script enchaine creation du modele, chargement, puis **execute les quatre
requetes** et affiche leurs resultats.

```bash
python -m pipeline.phase2_cassandra.demo_queries   # les 4 requetes seules
```
