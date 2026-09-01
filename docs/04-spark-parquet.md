# Phase 3 — Formatage Spark et Parquet

## Ce que fait cette phase

Cassandra sait repondre vite aux quatre questions pour lesquelles il a ete
modelise. Il ne sait pas repondre a « quel est le chiffre d'affaires total par
mois, tous rayons confondus ». Un `SUM` n'est possible qu'a l'interieur d'une
partition ; une agregation transverse imposerait de balayer tout le cluster.

**C'est exactement le trou que Spark comble.** Il lit l'integralite des
donnees en parallele, calcule les agregats que Cassandra refuse, et les ecrit
dans un format concu pour l'analyse : Parquet.

Les trois phases repondent donc a des besoins distincts :

| | Optimise pour | Question type |
|---|---|---|
| Oracle | la coherence en ecriture | « cette commande est-elle valide ? » |
| Cassandra | la lecture ciblee a grande echelle | « les commandes de CE client » |
| Spark / Parquet | l'analyse transverse | « le CA par mois sur deux ans » |

## Pourquoi Spark en local, sans conteneur

`master("local[*]")` : le pilote et les executeurs vivent dans un seul
processus JVM, qui utilise tous les coeurs disponibles. Pour ce volume de
donnees, l'execution locale evite le cout de coordination reseau d'un cluster
distribue, qui depasserait le gain de parallelisme.

**Le code reste identique a celui d'un cluster.** Passer sur un cluster reel ne
demanderait que de changer l'URL du maitre : aucune ligne de transformation ne
bougerait. C'est l'interet de l'abstraction DataFrame.

Cela sert aussi la contrainte memoire : pas de conteneur Spark, donc rien a
allumer en plus de Cassandra.

## Attention a la version de Java

Spark 3.5 est officiellement supporte sur Java 8, 11 et 17. Java 21 a ete
verifie comme fonctionnel sur ce projet et est accepte en repli. **Au-dela, non**
: les JVM recentes verrouillent l'acces reflexif a leurs internes, dont Spark
depend pour sa serialisation. L'echec se manifeste par des
`InaccessibleObjectException` illisibles, tres loin de la cause reelle.

Le Codespace utilise ici fournit Java 25 par defaut : le script bascule
automatiquement sur le JDK 21 installe via SDKMAN.

Le script `scripts/phase3_spark.sh` verifie la version courante, cherche un JDK
compatible parmi les emplacements habituels (`/usr/lib/jvm`, SDKMAN) et force
`JAVA_HOME` si necessaire. S'il n'en trouve aucun, il s'arrete avec la commande
d'installation a lancer, plutot que de laisser Spark echouer dans le vide.

## La lecture depuis Cassandra

```python
spark.read.format("org.apache.spark.sql.cassandra")
     .options(table="sales_by_category_month", keyspace="ecommerce").load()
```

Le connecteur decoupe la lecture suivant les **plages de jetons** de l'anneau :
chaque tache Spark lit un segment de l'espace des cles de partition. Sur un
vrai cluster, chaque executeur lirait en priorite les donnees du noeud dont il
est le plus proche.

C'est la seule phase ou une base reste allumee pendant un traitement. Spark
etant local, le pic memoire reste de l'ordre de 5 Go.

Le connecteur est declare en coordonnees Maven
(`com.datastax.spark:spark-cassandra-connector_2.12:3.5.1`) plutot qu'en jar
depose dans le depot : la version est visible dans le code et le livrable reste
leger. Le suffixe `_2.12` est la version de Scala avec laquelle Spark 3.5 est
compile — une version differente provoquerait des erreurs de methode
introuvable a l'execution.

## Les transformations

Fichier : `pipeline/phase3_spark/transforms.py`. Chaque fonction prend un
DataFrame et en renvoie un autre, sans effet de bord : elles s'enchainent et se
testent une par une.

### La regle metier retenue

```python
REVENUE_STATUSES = ["PAID", "SHIPPED", "DELIVERED"]
```

Une commande annulee ou retournee n'a jamais produit de recette ; une commande
en attente n'est pas encore payee. Les compter gonflerait le chiffre d'affaires
d'environ 17 % dans ce jeu de donnees.

**Les lignes ne sont pas supprimees pour autant** : elles restent dans la table
de faits, marquees par une colonne booleenne `is_revenue`, et une colonne
`net_amount` vaut zero pour elles. On peut ainsi analyser le taux d'annulation
sans avoir a recharger quoi que ce soit : on qualifie la donnee au lieu de la
filtrer.

### Les colonnes calendaires

`order_year`, `order_month`, `order_dow`, `order_hour` sont derivees de
l'horodatage. Les stocker dans Cassandra aurait impose de les ecrire pour
chaque ligne ; les deriver ici coute un seul balayage et donne a Kibana des
axes d'analyse directement exploitables.

### La segmentation RFM

Recence, Frequence, Montant : la segmentation client classique.

Les trois mesures sont converties en scores de 1 a 5 **par quintiles**
(`ntile(5)`) et non par seuils fixes. Consequence : la segmentation est
independante de la devise, du volume et de la periode. Elle reste valable si le
jeu de donnees change d'echelle, la ou des seuils en dur seraient a reregler.

Deux details de mise en oeuvre :

- **la recence est inversee** — peu de jours depuis le dernier achat est un bon
  signe, donc un score eleve. D'ou le `6 - score` ;
- **la date de reference est la derniere commande observee**, pas la date du
  jour. Sinon la segmentation vieillirait toute seule entre le calcul et la
  lecture du tableau de bord.

Six segments en sortent : Champions, Fideles, Nouveaux, A reconquerir,
Endormis, A surveiller.

Deux points de vigilance, verifies par les tests :

- **une ligne par client, et une seule.** Le regroupement porte sur le seul
  `customer_id`. Y ajouter les attributs descriptifs (fidelite, pays) serait
  tentant, mais il suffirait qu'un client ait commande depuis deux pays pour
  qu'il apparaisse en deux lignes, segmente sur des achats fractionnes ;
- **la segmentation ne couvre que les clients ayant genere du chiffre
  d'affaires.** Un client dont toutes les commandes sont annulees n'a pas de
  recence exploitable. L'index Elasticsearch des clients contiendra donc moins
  de documents que la table `customers` d'Oracle — ecart normal, a savoir
  expliquer.

### La mise en cache

```python
facts = T.build_fact_order_items(raw).cache()
```

La table de faits est relue par cinq traitements successifs. Sans cache, Spark
rejouerait la lecture Cassandra a chaque fois — les transformations sont
paresseuses, rien n'est calcule avant une action. Le cache transforme cinq
parcours reseau en un seul.

## Pourquoi Parquet

**Colonnaire.** Une requete qui ne lit que `net_amount` et `year_month` ne lit
que ces deux colonnes sur le disque. En JSON ou en CSV, il faut parcourir
chaque ligne en entier pour en extraire deux champs.

**Compresse.** Les valeurs d'une meme colonne sont homogenes, donc tres
compressibles. La compression `snappy` est retenue plutot que `gzip` : elle
decompresse beaucoup plus vite pour un taux a peine moindre — le bon arbitrage
pour un format destine a etre relu souvent.

**Typé.** Le schema est embarque dans le fichier. Pas de reinterpretation d'une
date ou d'un montant a chaque lecture, contrairement au CSV. Les montants sont
en `decimal(14,2)` : un `double` ne represente pas exactement 19,90 et l'erreur
s'accumulerait sur des centaines de milliers de lignes agregees.

**Filtrable.** Chaque fichier porte les valeurs min et max de ses colonnes. Un
filtre peut ecarter un fichier entier sans l'ouvrir : c'est le *predicate
pushdown*.

### Le partitionnement

```python
.partitionBy("order_year", "order_month")
```

Spark ecrit un repertoire par couple annee/mois :

```
fact_order_items/order_year=2025/order_month=11/part-00000.parquet
```

Une requete filtrant sur novembre 2025 ne lit **que ce repertoire**. C'est le
*partition pruning*, principal levier de performance du stockage colonnaire, et
l'equivalent conceptuel du bucketing temporel de la cle de partition Cassandra
— meme idee, appliquee au systeme de fichiers.

`spark.sql.shuffle.partitions` est ramene de 200 (le defaut) a 4 : sinon chaque
agregation produirait 200 fichiers de quelques kilo-octets. La multiplication
de petits fichiers est le probleme classique du stockage colonnaire, chacun
portant son propre en-tete et ses metadonnees.

Le meme piege se presente a l'ecriture partitionnee : Spark ecrit un fichier
par partition d'execution **et** par repertoire. La lecture Cassandra produisant
une partition par plage de jetons, on obtenait 143 fichiers de 46 Ko pour
24 repertoires. Une redistribution sur les colonnes de partitionnement, juste
avant l'ecriture, ramene le resultat a un fichier par repertoire.

Le gain ne se limite pas au nombre de fichiers : la table de faits est passee
de 6,6 Mo a 4,9 Mo, soit **25 % de moins pour exactement les memes donnees**.
Les encodages de Parquet -- dictionnaire, repetitions -- operent par bloc de
colonne : plus le bloc est grand, plus il y a de valeurs repetees a factoriser.
Beaucoup de petits fichiers, c'est autant d'en-tetes dupliques et de
dictionnaires trop courts pour amortir leur propre cout.

## Les sorties

| Fichier Parquet | Grain | Usage |
|---|---|---|
| `fact_order_items` | la ligne de commande | table de faits, indexee en phase 4 |
| `agg_sales_by_month` | le mois | courbe temporelle du dashboard |
| `agg_sales_by_category` | categorie x mois | repartition par rayon |
| `agg_top_products` | produit | classement par rayon |
| `dim_customers_rfm` | le client | segmentation, indexee en phase 4 |

Le script mesure et journalise le rapport de taille entre le JSON de la phase 1
et le Parquet produit, ce qui quantifie le gain du format colonnaire : de
l'ordre d'un facteur 15 sur ce jeu de donnees.

## Commandes

```bash
make phase3
make cassandra-down    # Cassandra n'est plus necessaire ensuite
```
