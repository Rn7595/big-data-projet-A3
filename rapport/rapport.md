# Projet Big Data — chaîne de traitement d'un jeu de données e-commerce

### D'une base Oracle normalisée à un tableau de bord Kibana

**Auteur :** [Prénom NOM]
**Formation :** [Classe / promotion]
**Enseignant :** [Nom de l'enseignant]
**Année universitaire :** [2025-2026]
**Dépôt du code :** archive jointe au présent rapport

---

## Sommaire

1. Le sujet et le parti pris
2. Architecture générale
3. Phase 1 — la source relationnelle Oracle
4. Phase 2 — dénormalisation et modèle Cassandra
5. Phase 3 — formatage Spark et Parquet
6. Phase 4 — indexation Elasticsearch et tableau de bord Kibana
7. Résultats mesurés et contrôles de cohérence
8. Difficultés rencontrées
9. Limites du travail et prolongements
10. Conclusion
11. Annexe — enchaînement des commandes

---

# 1. Le sujet et le parti pris

## 1.1 Ce qui était demandé

L'énoncé impose une chaîne complète en quatre étapes : construire une source de
données **SQL sous Oracle** avec une structure normalisée ; **dénormaliser** ces
données et les extraire vers **Cassandra** en passant par un fichier JSON ;
**formater** le résultat pour Spark ou Parquet en y appliquant des fonctions
d'analyse en Python ; enfin **indexer** dans Elasticsearch et exposer le résultat
dans **Kibana**. Le thème métier est libre.

L'enchaînement ne consiste pas seulement à empiler quatre technologies. Les
mêmes données traversent quatre modèles de stockage qui n'ont pas le même but :
Oracle sert la cohérence en écriture, Cassandra la lecture ciblée, Parquet
l'analyse sur l'ensemble des données, Elasticsearch la recherche libre. À chaque
passage on gagne quelque chose et on en perd une autre, et c'est ce que
j'explique dans ce rapport.

## 1.2 Le thème retenu

J'ai retenu un site de vente en ligne généraliste : des clients, leurs adresses,
un catalogue de produits organisé en rayons, et des commandes composées de
plusieurs lignes.

Ce thème contient les trois formes de relation qui rendent l'exercice
intéressant : une relation 1-N classique entre un client et ses commandes ; une
relation 1-N à cardinalité variable entre une commande et ses lignes, exactement
ce qu'une base orientée documents sait absorber et qu'une base relationnelle doit
éclater ; et une hiérarchie réflexive dans les catégories, qui n'a pas
d'équivalent direct en CQL. Un thème plus plat, des relevés de capteurs par
exemple, aurait rendu l'étape de dénormalisation presque triviale.

## 1.3 La contrainte d'exécution

L'ensemble devait tourner dans un GitHub Codespace de 16 Go de mémoire, partagés
avec l'éditeur et le système. Or Oracle, Cassandra et Elasticsearch sont trois
services lourds : les démarrer simultanément conduit à des évictions et à des
conteneurs tués par le noyau, sans message clair.

Toute l'architecture découle de cette contrainte. Le pipeline est conçu pour
s'exécuter **une phase à la fois**, chaque brique étant démarrée, utilisée, puis
éteinte avant la suivante. Subie au départ, cette contrainte s'est révélée
structurante : elle oblige à définir explicitement ce qui passe d'une phase à
l'autre, et donc à faire du fichier JSON un véritable contrat d'interface plutôt
qu'un détour décoratif.

---

# 2. Architecture générale

## 2.1 La chaîne

```
  PHASE 1             PHASE 2             PHASE 3             PHASE 4
+-----------+       +-----------+       +-----------+       +---------------+
|  Oracle   |       | Cassandra |       |   Spark   |       | Elasticsearch |
|   23ai    |       |    5.0    |       |   local   |       |       +       |
| 3NF, SQL  |       |  (NoSQL)  |       | (PySpark) |       |    Kibana     |
+-----------+       +-----------+       +-----------+       +---------------+
      |                   |                   |                     |
  JSON Lines         lecture CQL           Parquet              bulk API
      v                   v                   v                     v
  data/json/        tables denorm.      data/parquet/      index + dashboard
```

## 2.2 Comment on passe d'une phase à la suivante

Le passage de témoin n'est pas de même nature partout, et ce point conditionne la
possibilité même de l'exécution séquentielle.

Entre **Oracle et Cassandra**, le témoin est un fichier sur disque. Les deux
bases ne se connectent jamais l'une à l'autre : le chargeur de la phase 2 ne
connaît qu'un chemin de fichier, pas une connexion Oracle. C'est ce qui autorise
à éteindre Oracle avant de démarrer Cassandra, et c'est aussi ce que demande
l'énoncé lorsqu'il impose de produire un fichier JSON adapté à la structure
NoSQL. Entre **Cassandra et Spark**, il n'y a pas de fichier : Spark lit les
tables en direct via le connecteur officiel, seule étape où un service reste
actif pendant le traitement suivant. Entre **Spark et Elasticsearch**, on revient
à des fichiers, cette fois en Parquet.

| Phase | Conteneur allumé | Entrée | Sortie |
|-------|------------------|--------|--------|
| 1 | Oracle | — | `data/json/orders.jsonl`, `products.jsonl` |
| 2 | Cassandra | `data/json/` | tables du keyspace `ecommerce` |
| 3 | Cassandra (lecture) | Cassandra | `data/parquet/` |
| 4 | Elasticsearch + Kibana | `data/parquet/` | 2 index + tableau de bord |

## 2.3 Ce qui garantit réellement la séquentialité

Les services du `docker-compose.yml` portent tous un `profiles:`, si bien qu'un
`docker compose up` sans argument ne démarre rien : il faut nommer la phase. Je
précise que les profils **ne suffisent pas** à garantir la séquentialité : rien
n'interdit d'activer deux profils l'un après l'autre sans arrêter le premier. Ils
réduisent le risque d'un démarrage involontaire, sans plus.

La garantie effective repose sur trois éléments qui se complètent :

| Élément | Ce qu'il apporte |
|---|---|
| Profils Compose | sélection explicite des groupes de services |
| Gardes des scripts de phase | la phase 2 refuse de démarrer si Oracle tourne encore ; la phase 4 refuse si Oracle ou Cassandra tournent |
| Commandes d'arrêt entre les étapes | l'extinction effective, rappelée en fin de chaque script |

Les volumes sont nommés (`oracle_data`, `cassandra_data`, `es_data`,
`kibana_data`) : éteindre une phase n'efface pas ses données, et l'on peut
revenir sur une phase antérieure sans tout rejouer.

## 2.4 Budget mémoire

Les plafonds sont déclarés dans le `docker-compose.yml`. La dernière colonne est
un ordre de grandeur attendu, pas une mesure certifiée : je l'ai estimée à partir
du dimensionnement des services et de `docker stats`, mais elle dépend de la
machine.

| Phase | Conteneurs | `mem_limit` déclaré | Ordre de grandeur attendu |
|-------|-----------|---------------------|---------------------------|
| 1 | Oracle | 4 Go | 2 à 3 Go |
| 2 | Cassandra | 3 Go | ~2 Go (heap bornée à 1 Go) |
| 3 | Cassandra + Spark local | 3 Go + 2 Go | 4 à 5 Go |
| 4 | Elasticsearch + Kibana | 2,5 Go + 2 Go | 3 à 4 Go |

Cumulées, ces limites atteindraient 12 Go, auxquels s'ajouteraient Spark, Python
et le système : l'exécution séquentielle est donc ce qui rend le projet
réalisable dans l'enveloppe disponible, et non un raffinement de confort.

## 2.5 Organisation du dépôt

Le dépôt est organisé par phase (`sql/`, `cql/`, `pipeline/`, `scripts/`,
`kibana/`, `docs/`, `tests/`), ce qui évite les dépendances croisées : le code de
la phase 2 n'importe rien de la phase 1, il lit un fichier. Le dossier `docs/`
contient la documentation technique détaillée dont ce rapport est la synthèse.

---

# 3. Phase 1 — la source relationnelle Oracle

## 3.1 Le modèle

Huit tables, dix clés étrangères. Le `#` marque une clé étrangère.

```
COUNTRIES(country_code, country_name, region)
    ^
    |
ADDRESSES(address_id, #customer_id, address_type, street, city,
          postal_code, #country_code)
    ^                  ^
    |                  |
    |           CUSTOMERS(customer_id, email, first_name, last_name,
    |                     birth_date, signup_date, loyalty_tier)
    |                  ^
    |                  |
ORDERS(order_id, order_ref, #customer_id, order_date, order_status,
       #payment_method_id, #shipping_address_id, #billing_address_id,
       shipping_amount)
    ^                  ^
    |                  |
    |           PAYMENT_METHODS(payment_method_id, method_code, method_label)
    |
ORDER_ITEMS(#order_id, line_no, #product_id, quantity, unit_price,
            discount_pct)
                       ^
                       |
                PRODUCTS(product_id, sku, product_name, brand,
                         #category_id, unit_price, is_active, created_at)
                       ^
                       |
                CATEGORIES(category_id, category_code, category_name,
                           #parent_category_id)   <- cle etrangere reflexive
```

Le fichier de référence est `sql/01_schema.sql`.

## 3.2 La normalisation, forme par forme

**Première forme normale.** Aucun attribut multivalué. Un client peut avoir
plusieurs adresses : elles vivent dans une table dédiée, pas dans trois colonnes
`adresse_1`, `adresse_2`, `adresse_3` qu'il faudrait élargir le jour où un client
en déclare une quatrième. Une commande peut porter plusieurs produits : c'est
`ORDER_ITEMS`.

**Deuxième forme normale.** `ORDER_ITEMS` a une clé composite
`(order_id, line_no)`, et tous ses attributs dépendent de la ligne entière : la
quantité et le prix facturé n'ont de sens que pour cette ligne de cette commande.
Aucun attribut ne dépend du seul `order_id` ; s'il en existait un, il aurait sa
place dans `ORDERS`.

**Troisième forme normale.** C'est la raison d'être des deux tables de
référence. Si `ADDRESSES` portait directement une colonne `country_name`, on
aurait la dépendance transitive `address_id → country_code → country_name` : le
libellé « France » serait recopié des milliers de fois, et le renommer supposerait
de parcourir toute la table. Une table de huit lignes supprime le problème, et le
même raisonnement vaut pour `PAYMENT_METHODS`.

## 3.3 Trois choix qui méritent discussion

**Le montant total d'une commande n'est pas stocké.** Il se déduit des lignes.
Le stocker introduirait une redondance calculable et donc un risque
d'incohérence : il suffirait qu'une ligne soit corrigée sans recalcul du total
pour que la base se contredise elle-même. Ce total est calculé **une seule
fois**, au moment de la dénormalisation vers Cassandra. C'est là que les deux
modèles diffèrent : en relationnel on ne stocke pas ce qu'on peut recalculer,
alors qu'en NoSQL on calcule une fois à l'écriture pour ne plus avoir à le faire
à chaque lecture.

**`unit_price` apparaît dans deux tables, et ce n'est pas une redondance.**
`PRODUCTS.unit_price` est le prix courant du catalogue ;
`ORDER_ITEMS.unit_price` est le prix effectivement facturé ce jour-là. Ce sont
deux faits différents. Si le catalogue change demain, les factures d'hier ne
doivent pas bouger. Sans cette colonne, le chiffre d'affaires historique serait
faux à chaque changement de tarif. C'est de l'historisation, pas de la
duplication.

**`order_status` est une contrainte `CHECK`, pas une table de référence.** Le
domaine est fermé, connu à la conception, et ne porte aucun attribut propre :
une table de référence n'apporterait qu'une jointure de plus. `COUNTRIES` et
`PAYMENT_METHODS` portent, eux, de vrais attributs. Le critère appliqué n'est
donc pas « est-ce une valeur répétée ? » mais « cette valeur porte-t-elle des
attributs qui lui sont propres ? ».

## 3.4 Les index

Huit index sont déclarés dans `sql/02_indexes.sql`, pour les accès effectivement
utilisés par le projet. Ils peuvent accélérer les recherches et certaines
jointures selon le plan retenu par l'optimiseur ; pour une extraction de masse,
un balayage complet reste souvent le meilleur choix, et je n'ai pas cherché à
l'empêcher.

Un index mérite un commentaire particulier. `ix_orders_customer_date
(customer_id, order_date DESC)` est le miroir exact de la future table Cassandra
`orders_by_customer` : même clé d'accès, même ordre de tri. La différence est
qu'Oracle passe par une structure annexe, l'index, tandis que Cassandra rangera
directement les lignes dans cet ordre sur le disque. Même besoin métier, deux
mises en œuvre.

`ORDER_ITEMS(order_id)` n'est volontairement pas indexé séparément : la colonne
est déjà le préfixe de la clé primaire composite.

## 3.5 Le jeu de données : pourquoi le générer

Volumétrie pilotée par le fichier `.env` :

| Table | Volume |
|-------|--------|
| `countries` / `payment_methods` / `categories` | 8 / 5 / 40 (8 rayons + 32 feuilles) |
| `products` | 800 |
| `customers` | 5 000 |
| `addresses` | ~11 200 (1 facturation + 1 ou 2 livraisons par client) |
| `orders` | 60 000 |
| `order_items` | ~149 000 |

J'ai préféré générer les données plutôt qu'en télécharger. Les jeux publics
d'e-commerce sont presque toujours livrés à plat, déjà dénormalisés : les charger
aurait consisté à sauter l'étape que le sujet demande justement de réaliser. La
génération permet en outre d'injecter délibérément les défauts que l'étape de
nettoyage doit corriger, et la graine aléatoire (`SEED=42`) rend le jeu
reproductible, ce qui autorise à comparer les comptages d'une phase à l'autre.

Le réalisme du générateur n'est pas cosmétique. Trois propriétés sont injectées
volontairement, chacune pour une raison précise en aval : une **saisonnalité
mensuelle**, avec un pic de novembre-décembre au double du niveau moyen et un
creux en août, sans laquelle la courbe temporelle du tableau de bord serait une
ligne plate ; une **loi de Pareto** sur les produits comme sur les clients,
environ 20 % des produits concentrant 70 % des lignes, déséquilibre qui donne son
sens à la segmentation RFM de la phase 3 et rend visible en phase 2 le fait que
les partitions Cassandra ne sont pas de tailles égales ; et un **profil horaire**
avec un pic en soirée, qui rend l'histogramme par heure exploitable.

Les fourchettes de prix sont définies par **type de produit** et non par
catégorie. Ce détail vient d'une correction : avec une fourchette unique par
catégorie, « Périphériques » allant de 19 à 549 €, une webcam pouvait ressortir à
500 € et un écran 27 pouces à 20 €, ce qui rendait le tableau des meilleures
ventes absurde à la lecture. Chaque catégorie feuille porte donc trois à quatre
types de produits, chacun avec sa propre fourchette.

## 3.6 Le nettoyage et les contrôles

Les contraintes du schéma rejettent les erreurs **structurelles** à l'insertion :
une clé étrangère orpheline, une quantité négative, un statut hors domaine. Elles
sont en revanche aveugles à ce qui est syntaxiquement valide mais sémantiquement
sale : un email en majuscules, un nom entouré d'espaces, un double espace dans un
libellé produit. Le générateur injecte donc environ 1 % d'anomalies de ce type,
pour que l'étape de nettoyage produise un effet mesurable et ne soit pas un
passage à vide : sur l'exécution de référence, **441 anomalies** ont été
corrigées.

Cinq règles de normalisation sont appliquées (`sql/20_nettoyage.sql`) : emails en
minuscules, noms de clients, libellés produits, villes et codes postaux mis en
forme. Chaque instruction est nommée par un marqueur `-- @name:` que le lanceur
Python exploite pour journaliser le nombre de lignes corrigées par règle. Sans
cela, on ne saurait pas ce que le nettoyage a réellement fait.

Viennent ensuite huit contrôles (`sql/21_controles.sql`), dont six bloquants : si
l'un d'eux remonte une ligne, le pipeline s'arrête avant l'extraction. Deux
d'entre eux expriment des règles qu'**aucune contrainte déclarative ne peut
porter** dans ce schéma :

- `adresse_livraison_etrangere_au_client` — une clé étrangère garantit que
  l'adresse de livraison existe, pas qu'elle appartient au client de la commande ;
- `commande_anterieure_a_inscription` — une commande ne peut pas précéder
  l'inscription de son client. Un `CHECK` ne sait pas comparer deux colonnes
  situées dans deux tables différentes.

Deux contrôles sont purement informatifs : `commandes_sans_ligne` et
`clients_sans_commande`. Le premier a une importance particulière, sur laquelle
je reviens en section 7 : c'est lui qui explique l'écart entre 60 000 commandes
et le nombre de documents extraits.

Le nettoyage est fait **avant** l'extraction, volontairement. Une donnée sale qui
part dans le JSON se retrouve ensuite dans Cassandra, puis dans Parquet, puis
dans Elasticsearch, et il devient très difficile de remonter à sa source.

## 3.7 La requête de dénormalisation

C'est le cœur de l'étape 2 du sujet, et le fichier `sql/10_extract_orders.sql` est
sans doute la pièce la plus importante du projet. La requête transforme huit
tables relationnelles en un document JSON autonome par commande, avec ses lignes
imbriquées.

Le point que je souhaite souligner : **la transformation est faite par Oracle**,
en SQL/JSON normalisé (`JSON_OBJECT`, `JSON_ARRAYAGG`). Python ne fait que lire
des lignes et les écrire sur disque ; il ne reconstruit aucune structure. La
logique reste là où sont les données, ce qui évite de charger 60 000 commandes en
mémoire pour les réassembler côté client.

Trois détails techniques ont demandé de la mise au point. **`RETURNING CLOB` est
indispensable** : sans lui, `JSON_OBJECT` et `JSON_ARRAYAGG` retournent par
défaut un `VARCHAR2` limité à 4 000 octets, et le CLOB seul permet de produire
les documents volumineux. **`FORMAT JSON`** signale à `JSON_OBJECT` que le
tableau `items` est déjà du JSON ; sans ce mot-clé, il serait inséré comme une
chaîne échappée et le document deviendrait inutilisable. Enfin les agrégats
`total_amount`, `total_quantity` et `items_count`, volontairement absents du
schéma Oracle, sont matérialisés ici, à l'écriture.

Le format de sortie est du **JSON Lines**, un document complet par ligne, et non
un unique tableau JSON. La différence est pratique : le fichier se lit en flux
sans être chargé en mémoire, il se découpe trivialement puisque l'unité est la
ligne, et une interruption ne corrompt que la dernière ligne au lieu d'invalider
tout le document. C'est aussi le format que Spark lit nativement en parallèle.

Voici la structure d'un document produit. Les identifiants et les libellés
ci-dessous sont des valeurs d'illustration, pas une ligne réelle du fichier :

```json
{
  "order_id": 20114, "order_ref": "CMD-020114",
  "order_date": "2026-04-12T19:24:00", "order_year_month": "2026-04",
  "order_status": "DELIVERED",
  "payment_method": "CB", "payment_label": "Carte bancaire",
  "shipping_amount": 4.90,
  "customer": { "customer_id": 2087, "email": "...", "first_name": "...",
                "last_name": "...", "loyalty_tier": "SILVER",
                "signup_date": "2024-11-03" },
  "shipping_address": { "address_id": 4915, "city": "Lyon",
                        "postal_code": "69003", "country_code": "FR",
                        "country_name": "France", "region": "Europe" },
  "items": [
    { "line_no": 1, "product_id": 517, "sku": "SKU-000517",
      "product_name": "Casque bluetooth", "brand": "...",
      "category_id": 202, "category_name": "Casques audio",
      "parent_category_id": 2, "parent_category_name": "Image & Son",
      "quantity": 1, "unit_price": 99.00, "discount_pct": 10,
      "line_amount": 89.10 },
    { "line_no": 2, "product_id": 133, "sku": "SKU-000133",
      "product_name": "Cle USB", "brand": "...",
      "category_id": 104, "category_name": "Stockage",
      "parent_category_id": 1, "parent_category_name": "Informatique",
      "quantity": 2, "unit_price": 24.50, "discount_pct": 0,
      "line_amount": 49.00 }
  ],
  "items_count": 2, "total_quantity": 3, "total_amount": 143.00
}
```

Le total se vérifie à la main : 89,10 + 49,00 pour les articles, plus 4,90 de
frais de port, soit 143,00 €.

Tout ce dont Cassandra aura besoin pour répondre à ses quatre requêtes est là,
dans un seul document : le client, l'adresse, les lignes avec leur produit, leur
marque et leur catégorie, et les totaux déjà calculés. Aucune jointure ne sera
nécessaire à la lecture.

---

# 4. Phase 2 — dénormalisation et modèle Cassandra

## 4.1 Un renversement complet de méthode

Cette phase répond à une seule question : comment passe-t-on d'un schéma conçu
pour éviter la redondance à un schéma conçu pour éviter les jointures ?

| | Oracle | Cassandra |
|---|---|---|
| Point de départ | les entités du domaine | les requêtes à servir |
| Règle | ne jamais dupliquer | dupliquer autant que nécessaire |
| Jointure | le moteur la fait | elle n'existe pas |
| Tri | calculé à la lecture | écrit sur le disque |
| Agrégat | calculé à la lecture | calculé à l'écriture |

En SQL, on modélise les entités puis on écrit n'importe quelle requête : le
moteur se débrouille. En Cassandra, **une table répond à une requête**. Si une
cinquième requête apparaît, on crée une cinquième table. J'ai donc commencé par
écrire les quatre questions auxquelles le modèle devait répondre, et seulement
ensuite les tables.

## 4.2 Le mécanisme de la clé primaire

Tout le reste en découle, il faut donc le poser clairement. Dans
`PRIMARY KEY ((clé_de_partition), clustering_1, clustering_2)`, la clé de
partition est hachée et désigne le nœud qui détient la donnée, tandis que les
colonnes de clustering fixent l'ordre de tri **sur le disque** à l'intérieur de
la partition.

Deux conséquences pratiques gouvernent toute la modélisation : une lecture
**doit** fournir la clé de partition complète, faute de quoi Cassandra doit
interroger tous les nœuds ; et un tri obtenu par la clé de clustering ne coûte
rien à l'exécution, puisqu'il est déjà inscrit dans l'ordre physique des lignes.

## 4.3 Les quatre tables

**Q1 — l'historique d'un client, les plus récentes d'abord.**

```cql
PRIMARY KEY ((customer_id), order_date, order_id)
WITH CLUSTERING ORDER BY (order_date DESC, order_id DESC)
```

`customer_id` en clé de partition pour trois raisons cumulées : il prend 5 000
valeurs distinctes, donc les données se répartissent uniformément au lieu de
s'entasser au même endroit ; la taille de chaque partition reste petite, un très
gros client dépassant rarement quelques centaines de commandes ; et surtout elle
correspond à la question posée, qui connaît toujours le client dont elle veut
l'historique.

Le tri décroissant par date est écrit sur le disque : « les dix dernières
commandes » lit les dix premières lignes de la partition et s'arrête. `order_id`
est ajouté en dernière position de clustering pour l'unicité : deux commandes
passées à la même seconde par le même client auraient sinon la même clé, et la
seconde écraserait silencieusement la première.

**Q2 — une commande par son identifiant.**

```cql
PRIMARY KEY (order_id)
```

Ce sont les **mêmes données** que Q1, dans une seconde table : le principe « une
table par requête » appliqué jusqu'au bout, et sans doute ce qui surprend le plus
quand on vient du relationnel. Interroger `orders_by_customer` n'est pas une
option, puisqu'une lecture exige la clé de partition : chercher une commande par
son seul identifiant dans une table partitionnée par client obligerait à balayer
toutes les partitions. Cette seconde table coûte un doublement du volume
stocké et une écriture supplémentaire à chaque commande. C'est le compromis que
le modèle assume : on accepte de stocker deux fois pour ne jamais avoir à
chercher partout.

**Q3 — le montant des articles d'une catégorie sur un mois.**

```cql
PRIMARY KEY ((category_id, year_month), order_date, order_id, line_no)
```

C'est le point le plus intéressant du modèle, parce que la clé de partition y est
composite.

Partitionner par la seule `category_id` donnerait 32 partitions qui grossiraient
indéfiniment au fil des mois, jusqu'à ce qu'une catégorie populaire dépasse la
limite pratique : c'est l'**anti-pattern de la partition non bornée**, le plus
fréquent en modélisation Cassandra, et il ne se voit pas sur un jeu de
démonstration — il se voit deux ans après la mise en production.

Ajouter le mois dans la clé de partition est un **bucketing temporel** : la
partition est bornée par construction puisqu'elle ne contient qu'un mois, et le
nombre de partitions croît avec le temps, ce qui est le comportement souhaitable
d'un système distribué, la charge se répartissant sur de nouveaux nœuds au lieu de
s'accumuler sur les mêmes. Effet secondaire recherché, la requête métier porte
justement sur un couple (catégorie, mois) : elle lit une partition et une seule.

La granularité retenue est la ligne de commande et non la commande, si bien
qu'une commande touchant trois catégories alimente trois partitions différentes.
C'est ce qui permet d'imputer chaque euro à sa catégorie, et c'est cette table
que Spark lira en phase 3 comme table de faits.

Une précision de périmètre, importante pour la lecture des résultats : cette
table somme `line_amount` **tous statuts confondus**, hors frais de port. Le
chiffre d'affaires net calculé plus loin par Spark applique en plus une sélection
sur les statuts. Ces deux indicateurs ne recouvrent pas le même périmètre et ne
doivent pas être comparés directement.

**Q4 — le catalogue d'une catégorie.**

```cql
PRIMARY KEY ((category_id), product_name, product_id)
```

Le tri alphabétique est obtenu par la clé de clustering, sans `ORDER BY`. La
hiérarchie des catégories, exprimée en SQL par une clé étrangère réflexive, est
ici aplatie en deux colonnes : CQL n'a pas d'équivalent de la requête récursive.
C'est une perte d'expressivité que la phase 1 assumait sans difficulté.

## 4.4 Les anti-patterns écartés

J'ai envisagé puis écarté trois modélisations alternatives. Les mentionner me
paraît plus utile que de ne présenter que la solution retenue.

| Choix envisagé | Pourquoi il est mauvais |
|---|---|
| Partitionner par `order_date` | toutes les commandes d'une même journée se retrouvent au même endroit, qui encaisse seul les écritures du jour. |
| Partitionner par `order_status` | six valeurs distinctes seulement, donc six partitions énormes et très déséquilibrées. |
| Index secondaire sur `customer_id` | un index secondaire Cassandra n'a pas le comportement d'un index SQL : la lecture doit parcourir toute la table au lieu de viser une partition. |

## 4.5 Le type utilisateur `order_item`

```cql
items list<frozen<order_item>>
```

C'est **la** dénormalisation. La relation 1-N entre `ORDERS` et `ORDER_ITEMS`,
qui exigeait une table séparée et une jointure en SQL, devient une collection
imbriquée dans la ligne de commande. Le nom du produit, sa marque et sa catégorie
sont recopiés à l'intérieur : en SQL, il aurait fallu deux jointures
supplémentaires pour les obtenir.

Le mot-clé `frozen` veut dire que Cassandra stocke toute la liste comme une seule
valeur. Pour modifier une seule ligne de commande, il faut donc réécrire la liste
entière. C'est acceptable ici parce qu'**une commande passée ne change plus** ;
ce serait un mauvais choix pour une donnée mise à jour souvent.

## 4.6 Le chargement

Le chargeur (`pipeline/phase2_cassandra/load_json.py`) ne connaît aucune
connexion Oracle : il lit `data/json/`. C'est ce qui rend l'exécution séquentielle
possible, et le script de phase refuse d'ailleurs de démarrer si le conteneur
Oracle tourne encore.

Deux choix techniques méritent d'être justifiés.

**Pas de `BatchStatement`.** C'est un réflexe venu du SQL, et une mauvaise idée
ici : un batch Cassandra sert à garantir l'atomicité à l'intérieur d'une
partition, ce n'est pas un outil de performance, et il devient plus lent que les
écritures individuelles dès qu'il touche plusieurs partitions. J'utilise
`execute_concurrent_with_args`, qui envoie soixante-quatre requêtes en
parallèle.

**Types monétaires en `decimal`, jamais en `double`.** Un `double` ne représente
pas exactement 19,90, et l'erreur s'accumule sur des centaines de milliers de
lignes agrégées. Ce choix est appliqué de bout en bout : `decimal` en Cassandra,
`decimal(14,2)` en Parquet, `scaled_float` en Elasticsearch.

En fin de chargement, le script compte les lignes de chaque table et les compare
au nombre de lignes écrites. Un `COUNT(*)` sans clé de partition est précisément
le balayage que tout le modèle cherche à éviter : c'est acceptable pour un
contrôle ponctuel, jamais en usage courant.

## 4.7 Ce que le modèle fait perdre

Un modèle NoSQL orienté requêtes a un coût, qu'il serait malhonnête de passer
sous silence.

L'**intégrité référentielle** disparaît : il n'y a plus aucune clé étrangère. Si
un nom de produit change, les commandes passées gardent l'ancien. Ici c'est
voulu, puisque c'est de l'historisation, mais plus rien ne l'impose
techniquement. Les **requêtes imprévues** deviennent coûteuses : toute question
non anticipée exige une nouvelle table et un rechargement, là où en SQL il aurait
suffi d'écrire une requête. La **cohérence entre les copies** n'est plus
automatique, puisque la même commande existe dans deux tables et qu'une écriture
partielle peut les désynchroniser : il faut des contrôles applicatifs, ce qui
explique les comptages du chargeur et le traceur présenté en section 7. Enfin les
**analyses transverses** ne sont pas le terrain de ce modèle, une agrégation non
ciblée pouvant balayer toute la table. C'est exactement le vide que la phase
suivante vient combler.

---

# 5. Phase 3 — formatage Spark et Parquet

## 5.1 Ce que cette phase apporte

Cassandra répond très bien aux quatre requêtes prévues, et mal à « le chiffre
d'affaires par mois sur deux ans », qui ne cible aucune partition. Spark lit les
données en parallèle, calcule les agrégats et les écrit dans un format conçu pour
l'analyse. Les trois systèmes répondent donc à des besoins distincts, et c'est la
raison d'être de la chaîne :

| | Optimisé pour | Question type |
|---|---|---|
| Oracle | la cohérence en écriture | « cette commande est-elle valide ? » |
| Cassandra | la lecture ciblée à grande échelle | « les commandes de CE client » |
| Spark / Parquet | l'analyse transverse | « le CA par mois sur deux ans » |

## 5.2 Spark en local, sans conteneur

`master("local[*]")` : Spark tourne dans un seul processus Java qui utilise tous
les cœurs de la machine. Pour un volume de cette taille, faire tourner un cluster
coûterait plus en coordination que ce qu'il ferait gagner. Cela sert aussi la
contrainte mémoire, puisqu'il n'y a aucun conteneur Spark à allumer en plus de
Cassandra. À noter : **le code serait le même sur un cluster**, il faudrait
seulement changer l'URL du maître, sans modifier une ligne de transformation.

La lecture depuis Cassandra utilise le connecteur officiel, déclaré en
coordonnées Maven (`com.datastax.spark:spark-cassandra-connector_2.12:3.5.1`)
plutôt qu'en jar déposé dans le dépôt : la version reste visible dans le code et
le livrable reste léger. Le connecteur découpe la lecture en plusieurs tâches qui
se partagent les partitions de la table, ce qui permet à Spark de lire en
parallèle plutôt que ligne à ligne.

## 5.3 Les transformations et la règle métier

Chaque fonction de `pipeline/phase3_spark/transforms.py` prend un DataFrame et en
renvoie un autre, sans effet de bord. Elles s'enchaînent, et surtout elles se
testent une par une, ce qui a servi, comme je l'explique en section 8.

La règle métier centrale tient en une ligne :

```python
REVENUE_STATUSES = ["PAID", "SHIPPED", "DELIVERED"]
```

Dans la règle simplifiée retenue pour ce projet, les commandes annulées,
retournées ou en attente ne contribuent pas au chiffre d'affaires net des
articles, qui exclut par ailleurs les frais de port. Les compter gonflerait le
chiffre d'affaires d'environ 17 % sur ce jeu de données.

Les lignes correspondantes ne sont pas supprimées pour autant : elles restent
dans la table de faits, marquées par une colonne booléenne `is_revenue`, avec un
`net_amount` à zéro. On peut donc analyser le taux d'annulation sans rien
recharger. J'ai préféré **marquer les lignes plutôt que les supprimer**, parce
qu'une donnée supprimée en phase 3 n'existe plus en phase 4.

Les colonnes calendaires (`order_year`, `order_month`, `order_dow`,
`order_hour`) sont dérivées de l'horodatage à ce moment-là : les stocker dans
Cassandra aurait imposé de les écrire pour chaque ligne, les dériver ici coûte un
seul balayage.

Un mot sur la définition des indicateurs mensuels, parce que l'ambiguïté est
facile. Dans `agg_sales_by_month`, le chiffre d'affaires porte sur les articles
après remise, hors frais de port, pour les trois statuts retenus, tandis que les
commandes, articles et clients sont comptés **tous statuts confondus** : ils
décrivent l'activité, pas la recette. Les noms de colonnes ont été choisis en
conséquence — `nb_lignes_sans_ca` plutôt que `nb_lignes_annulees`, puisque le
compte inclut aussi les commandes en attente, et `ca_moyen_par_commande` plutôt
que `panier_moyen`, puisque le dénominateur compte toutes les commandes du mois.

## 5.4 La segmentation RFM

Récence, Fréquence, Montant : la segmentation client classique, et l'endroit du
projet où les données servent enfin à répondre à une question de gestion.

Les trois mesures sont converties en scores de 1 à 5 **par quintiles**
(`ntile(5)`) et non par seuils fixes. La conséquence est importante : la
segmentation devient indépendante de la devise, du volume et de la période. Elle
reste valable si le jeu de données change d'échelle, là où des seuils écrits en
dur seraient à re-régler.

Deux détails de mise en œuvre :

- **la récence est inversée.** Peu de jours écoulés depuis le dernier achat est
  un bon signe, donc un score élevé : d'où le `6 - score` ;
- **la date de référence est la dernière commande observée**, pas la date du
  jour. Sinon la segmentation vieillirait toute seule entre le calcul et la
  consultation du tableau de bord, et tous les clients glisseraient
  progressivement vers « endormis » sans que rien n'ait changé.

Six segments en sortent : Champions, Fidèles, Nouveaux, À reconquérir, Endormis,
À surveiller.

Deux propriétés expliquent des écarts de comptage visibles plus loin. Le
regroupement porte sur le **seul** `customer_id` : y ajouter le pays ou le niveau
de fidélité serait tentant, mais il suffirait qu'un client ait commandé depuis
deux pays pour qu'il apparaisse en deux lignes, segmenté sur des achats
fractionnés. Et la segmentation ne couvre que les clients ayant généré du chiffre
d'affaires, un client dont toutes les commandes sont annulées n'ayant pas de
récence exploitable : l'index Elasticsearch des clients contient donc nettement
moins de documents que la table `customers` d'Oracle : 3 439 contre 5 000 sur
l'exécution de référence.

La table de faits est enfin mise en cache (`.cache()`) parce qu'elle est relue
par cinq traitements successifs ; les transformations Spark étant paresseuses,
sans cache la lecture Cassandra serait rejouée cinq fois.

## 5.5 Pourquoi Parquet

Quatre propriétés, qui se complètent. **Colonnaire** : une requête qui ne lit que
`net_amount` et `year_month` ne lit que ces deux colonnes sur le disque, là où
JSON et CSV imposent de parcourir chaque ligne entière pour en extraire deux
champs. **Compressé** : les valeurs d'une même colonne sont homogènes, donc très
compressibles ; j'ai retenu `snappy` plutôt que `gzip`, car elle décompresse
beaucoup plus vite pour un taux à peine moindre, le bon arbitrage pour un format
destiné à être relu souvent. **Typé** : le schéma est embarqué dans le fichier,
sans réinterprétation d'une date ou d'un montant à chaque lecture, contrairement
au CSV, et les montants sont en `decimal(14,2)`. **Filtrable** : chaque fichier
porte les valeurs minimale et maximale de ses colonnes, ce qui permet d'écarter
un fichier entier sans même l'ouvrir.

Le partitionnement suit la même logique que le bucketing temporel de Cassandra,
appliquée cette fois au système de fichiers :

```
fact_order_items/order_year=2025/order_month=11/part-00000.parquet
```

Une requête filtrant sur novembre 2025 ne lit que ce répertoire. J'ai aussi
ramené `spark.sql.shuffle.partitions` de 200, la valeur par défaut, à 4 : sans
cela chaque agrégation produirait deux cents fichiers de quelques kilo-octets. La
multiplication des petits fichiers est le piège classique du stockage colonnaire,
et c'est ce qui m'a coûté le plus de temps sur cette phase (section 8).

Les sorties de la phase :

| Fichier Parquet | Grain | Usage |
|---|---|---|
| `fact_order_items` | la ligne de commande | table de faits, indexée en phase 4 |
| `agg_sales_by_month` | le mois | référence pour vérifier les totaux mensuels |
| `agg_sales_by_category` | catégorie × mois | référence pour la répartition par rayon |
| `agg_top_products` | le produit | référence pour le classement des ventes |
| `dim_customers_rfm` | le client | segmentation, indexée en phase 4 |

Seuls deux de ces cinq fichiers sont indexés en phase 4. Les trois agrégats
restent en Parquet et servent de point de comparaison pour vérifier les chiffres
affichés par Kibana ; la raison de ce choix est expliquée en section 6.2.

---

# 6. Phase 4 — indexation Elasticsearch et tableau de bord Kibana

## 6.1 Ce que cette phase apporte

Parquet est excellent pour calculer, médiocre pour chercher : répondre à « les
commandes de ce client en novembre » impose de lire les fichiers concernés.
Elasticsearch inverse le compromis. Il indexe chaque champ pour répondre en
quelques millisecondes à des filtres et des agrégations arbitraires, sur des
questions que l'utilisateur n'a pas annoncées à l'avance. C'est le passage d'un
format d'analyse à un format d'exploration.

## 6.2 Deux index, et le choix du grain

| Index | Grain | Documents |
|---|---|---|
| `ecom-order-items` | la ligne de commande | ~149 000 |
| `ecom-customers` | le client | ~3 400 |

J'ai indexé le grain **le plus fin** plutôt que les agrégats déjà calculés, et
c'est un choix délibéré. Elasticsearch sait agréger lui-même : en indexant la
ligne de commande, Kibana peut construire n'importe quel regroupement : par mois,
par marque, par pays, par heure, ou par combinaison des quatre. Un index
pré-agrégé par mois ne répondrait qu'aux questions prévues d'avance, et il
faudrait réindexer à chaque nouvelle question. Les agrégats de la phase 3 ne sont
donc **volontairement pas** indexés : ils feraient double emploi et
introduiraient un risque d'incohérence entre deux sources du même chiffre. Ils
restent en Parquet, où ils servent de référence pour vérifier les totaux affichés
par Kibana.

## 6.3 Les mappings

Le principe retenu est simple : **le mapping est déclaré, jamais deviné.**

| Type retenu | Champs | Raison |
|---|---|---|
| `keyword` | `brand`, `category_name`, `country_name`, `order_status`, `segment` | valeur atomique, non analysée : c'est ce qui rend le regroupement exact possible |
| `text` + sous-champ `keyword` | `product_name` | le texte pour la recherche plein texte, le sous-champ pour l'agrégation |
| `scaled_float` (facteur 100) | tous les montants | stocke un entier de centimes en interne : précision exacte au centime, empreinte réduite par rapport à un `double` |
| `byte` / `short` | `order_month`, `line_no`, scores RFM | le domaine est connu et petit |
| `date` | `order_date`, `derniere_commande` | permet les histogrammes temporels de Kibana |

Deux réglages du mapping méritent un mot. **`dynamic: strict`** fait échouer
l'indexation d'un champ non déclaré au lieu de l'accepter silencieusement : une
colonne ajoutée en amont sans mise à jour du mapping se voit immédiatement,
plutôt que d'apparaître trois semaines plus tard sous un type aberrant.
**`number_of_replicas: 0`** parce que sur une installation à un seul nœud une
réplique ne peut être placée nulle part, et l'index resterait indéfiniment en
état `yellow`.

## 6.4 Des identifiants dérivés des clés métier

```python
"_id": f"{order_id}-{line_no}"
```

L'identifiant du document est dérivé des clés métier plutôt que généré par
Elasticsearch. Conséquence : réindexer met à jour les documents existants au lieu
de créer des doublons. Le script recrée toutefois les index avant le chargement
complet, si bien qu'un nouveau lancement remplace franchement les résultats
précédents plutôt que de les compléter à moitié.

## 6.5 Les contrôles de la phase

Le script ne se contente pas d'indexer. Il compare le nombre de documents envoyés
au nombre de documents présents dans l'index après `refresh`, l'indexation étant
asynchrone par défaut : sans ce rafraîchissement le comptage porterait sur un
index encore partiellement invisible. Puis il recalcule le chiffre d'affaires
total par une agrégation Elasticsearch, qui doit coïncider avec celui produit par
Spark. La portée exacte de ce contrôle mérite d'être précisée : il rapproche deux
agrégations du même champ `net_amount` et ne recalcule pas la règle métier depuis
Oracle. Il détecte une perte de documents ou une troncature de valeurs, pas une
erreur de définition.

## 6.6 Le tableau de bord

Le tableau de bord est **généré par code** plutôt que construit à la souris : sa
définition, versionnée dans le dépôt, permet de le reconstruire à l'identique sur
une installation neuve.

Huit panneaux, choisis pour répondre aux questions d'un responsable e-commerce :

| Panneau | Type | Question |
|---|---|---|
| Chiffre d'affaires net | métrique | combien ai-je vendu ? |
| Commandes facturées | métrique | sur combien de commandes ? |
| CA par mois | barres | quelle saisonnalité ? |
| Répartition par rayon | anneau | quelle part pour chaque univers ? |
| Meilleures ventes | table | quels produits portent le CA ? |
| CA par pays | barres | où sont mes clients ? |
| Statuts de commande | barres | combien de lignes par statut ? |
| Segmentation RFM | barres | à qui ai-je affaire ? |

Trois précautions de lecture, qu'il vaut mieux énoncer que laisser deviner : le
CA net correspond aux articles après remise, hors frais de port ; le compteur de
commandes facturées utilise `unique_count`, qui est une **estimation** du nombre
de valeurs distinctes et non un comptage exact ; le graphique des statuts compte
des **lignes de commande**, pas des commandes distinctes, et ne donne donc pas
directement un taux d'annulation par commande.

Deux détails de conception, enfin. La vue de données des clients n'a
volontairement **pas** de champ temporel : la segmentation est un état à
l'instant du calcul, pas une série temporelle, et lui en donner un soumettrait le
panneau au sélecteur de période et le viderait dès que l'utilisateur restreint la
fenêtre. Et `timeRestore` est actif, ce qui enregistre la fenêtre des vingt-cinq derniers
mois avec le tableau de bord. Sans cela, Kibana l'ouvre sur « les quinze
dernières minutes » et tous les panneaux apparaissent vides.

---

# 7. Résultats mesurés et contrôles de cohérence

## 7.1 Les chiffres d'une exécution complète

Exécution de référence du 1er septembre :

| Mesure | Valeur |
|---|---|
| Lignes de commande, aux quatre étapes | **149 300** |
| Commandes générées | 60 000 |
| Commandes sans ligne (mesurées sur Oracle) | 277 |
| Documents JSON extraits | **59 723** |
| Produits / clients inscrits / clients segmentés | 800 / 5 000 / 3 439 |
| Anomalies corrigées au nettoyage | 441 |
| Chiffre d'affaires, Spark = Elasticsearch | **5 338 199,61 €** |
| Compression JSON → Parquet | **facteur 15,6** |
| Fichiers Parquet de la table de faits | 24 |

Temps mesurés : chargement des 149 300 lignes dans Oracle 3,1 s ; extraction des
59 723 documents 5,3 s ; chargement Cassandra 109,5 s ; lecture Spark 19,6 s ;
indexation Elasticsearch 21,8 s.

L'écart entre 60 000 commandes générées et 59 723 documents extraits n'est pas une
perte : ce sont les 277 commandes sans aucune ligne, écartées par la jointure
interne de la requête d'extraction et mesurées indépendamment sur Oracle.
L'égalité `60 000 − 277 = 59 723` est **vérifiée**, ce qui est très différent de
constater un écart et de l'expliquer après coup. Les comptages exacts varient par
ailleurs d'une exécution à l'autre, la fenêtre de vingt-quatre mois se terminant
au jour de l'exécution ; ce qui ne varie pas, et qui seul importe, ce sont les
égalités.

## 7.2 Les sept contrôles de cohérence

`make test` (`tests/test_coherence_pipeline.py`) compare les rapports enregistrés
par les quatre phases :

1. les lignes de commande sont en nombre identique dans Oracle, dans le JSON,
   dans Cassandra, dans Parquet et dans Elasticsearch ;
2. l'écart entre commandes générées et documents extraits est **exactement** égal
   au nombre de commandes sans ligne mesuré sur Oracle ;
3. le chiffre d'affaires calculé par Spark et celui recalculé par agrégation
   Elasticsearch coïncident ;
4. le nombre de documents envoyés à Elasticsearch est égal au nombre de documents
   effectivement présents dans l'index après rafraîchissement ;
5. les comptages de tables Cassandra correspondent aux lignes écrites par le
   chargeur ;
6. le nombre de clients segmentés est cohérent avec les clients ayant généré du
   chiffre d'affaires ;
7. le rapport de taille entre JSON et Parquet est mesuré et journalisé.

Sur l'exécution de référence, les sept passent.

## 7.3 Le traceur, contrôle complémentaire

`make test` compare des volumétries globales, et l'on peut imaginer deux erreurs
qui se compensent en laissant les totaux justes. J'ai donc ajouté un second
contrôle, de nature différente : `make tracer CMD=<id>` suit **une commande
précise** à travers chaque source disponible — Oracle, le fichier JSON,
Cassandra, Parquet, Elasticsearch — et compare son nombre de lignes et ses
montants au centime.

Sur l'exécution de référence, la commande 46463 du client 4317, d'un montant de
99,62 €, est identique dans toutes les sources interrogeables. Le traceur
distingue explicitement une source indisponible d'une commande absente, et
renvoie un code de sortie distinct lorsque moins de deux sources sont comparables
— autrement dit il refuse de conclure quand il n'a rien comparé.

## 7.4 Ce que ces contrôles ne prouvent pas

Il me paraît important de délimiter la portée de ces vérifications plutôt que
d'annoncer une validation générale.

`make test` **lit les rapports que le pipeline a lui-même écrits** dans
`data/reports/` ; il ne réinterroge pas les bases. Ses conclusions portent donc
sur l'exécution qui a produit ces rapports, et des rapports anciens ne prouvent
rien sur l'état actuel des bases. C'est une limite réelle du dispositif, et c'est
précisément pour cela que le traceur existe : lui relit les sources.

Cette limite n'est pas théorique. Lors d'une exécution, j'ai lancé les commandes
sans les enchaîner correctement : la phase 3 s'est interrompue, la phase 4 a
réindexé les fichiers Parquet de l'exécution précédente, et le pipeline n'a
affiché aucune erreur. C'est `make test` qui a détecté l'incohérence : 150 028
lignes côté Oracle et Cassandra, 149 186 côté Parquet et Elasticsearch. Le
contrôle a joué exactement le rôle attendu, sur un incident que je n'aurais pas
vu autrement.

Enfin, ces contrôles vérifient des égalités de volumétrie et de montant. Ils ne
vérifient pas champ par champ que chaque document est correct, et ils ne valident
pas la pertinence des règles métier retenues : ils garantissent que la même règle
est appliquée de bout en bout, pas qu'elle soit la bonne.

---

# 8. Difficultés rencontrées

Je consacre une section à ces incidents parce qu'ils ont, plus que le reste,
orienté la forme finale du projet.

**L'installation de PySpark.** `pip install pyspark` échouait sur
`AttributeError: install_layout`, une incompatibilité entre setuptools et le
`setup.py` de PySpark 3.5, dont le message n'oriente pas vers la cause. Le
contournement, documenté dans le README, consiste à épingler setuptools puis à
désactiver l'isolation de build.

**La version de Java.** Spark 3.5 est officiellement supporté sur Java 8, 11 et
17, et j'ai vérifié que 21 fonctionne également. Avec Java 25, que le Codespace
fournit par défaut, Spark échoue sur des erreurs internes à la JVM dont le
message ne renvoie pas à la cause. Le script de phase 3 détecte donc la version
courante, cherche un JDK compatible et force `JAVA_HOME` ; s'il n'en trouve
aucun, il s'arrête en affichant la commande d'installation plutôt que de laisser
Spark échouer dans le vide.

**Les 143 fichiers de 46 Ko.** L'écriture partitionnée de Spark produit un fichier
par partition d'exécution **et** par répertoire. Comme la lecture Cassandra est
elle-même découpée en plusieurs morceaux, j'obtenais 143 fichiers pour 24
répertoires. Une redistribution sur les colonnes de partitionnement, juste avant
l'écriture, ramène le résultat à un fichier par répertoire. Le gain ne se limite
pas au nombre de fichiers : la table de faits est passée de 6,6 Mo à 4,9 Mo, soit
**25 % de moins pour exactement les mêmes données**, la compression de Parquet
étant d'autant plus efficace que les fichiers sont gros.

**Un client compté deux fois dans la segmentation RFM.** C'est le test qui l'a
trouvé, pas la lecture du code. Le regroupement incluait initialement le pays et
le niveau de fidélité en plus de l'identifiant client ; il suffisait qu'un client
ait commandé depuis deux pays pour apparaître en deux lignes, chacune segmentée
sur une moitié de ses achats. Le total du chiffre d'affaires restait juste, si bien
que les contrôles de volumétrie ne voyaient rien. C'est pour ce genre d'erreur
que les transformations sont écrites comme des fonctions testables une par
une.

**L'import du tableau de bord Kibana.** L'API d'import groupé renvoyait une
erreur 500 sans détail exploitable. J'ai remplacé l'appel groupé par une création
objet par objet, ce qui isole l'objet fautif et rend l'erreur lisible. Le script
crée par ailleurs les vues de données **avant** de tenter le tableau de bord : en
cas d'échec, celui-ci reste constructible à la main dans l'interface, puis
réexportable vers le dépôt.

**Deux pannes silencieuses côté conteneurs.** Elasticsearch refuse de démarrer si
`vm.max_map_count` est inférieur à 262 144, et le défaut d'un Codespace est très
en dessous ; l'échec se produit **après** le démarrage du conteneur, si bien qu'on
ne voit qu'un conteneur qui s'arrête tout seul, sans explication. Le script de
phase 4 applique désormais le réglage automatiquement. De même, après une mise en
veille du Codespace, Cassandra apparaissait démarré mais son port CQL n'acceptait
plus de connexion, et la phase 3 échouait : les scripts attendent maintenant
l'état `healthy` défini par les `healthcheck` du Compose, avec un délai
d'attente explicite, plutôt qu'une temporisation fixe.

---

# 9. Limites du travail et prolongements

Plusieurs choix ont été faits pour tenir dans le cadre de l'exercice, et il me
semble plus utile de les nommer que de les laisser passer pour des propriétés du
système.

**Tout tourne sur une seule machine.** Cassandra est configuré avec un facteur
de réplication de 1 et Elasticsearch sans réplique : sur un nœud unique, il n'y a
rien à répliquer. Spark s'exécute en local. Le déploiement distribué n'a pas été
testé, et je me garde donc d'affirmer des performances à l'échelle que je n'ai
pas observées. Les valeurs de réplication seraient à revoir sur une installation
réelle, mais c'est un travail que ce projet n'a pas fait.

**Le pipeline est rejoué en entier, jamais en incrémental.** Un vrai système
chargerait les nouvelles commandes en s'appuyant sur un horodatage ou un journal
de modifications, sans recalculer deux ans d'historique. C'est le prolongement le
plus naturel de ce travail.

**Les données sont synthétiques.** Elles sont réalistes par construction, ce qui
signifie aussi qu'elles sont propres par construction, en dehors des anomalies que
j'y ai injectées volontairement. Un jeu réel apporterait des cas que je n'ai pas
anticipés, et c'est précisément ce qu'un générateur ne peut pas simuler
honnêtement.

**L'orchestration reste manuelle.** L'enchaînement repose sur des commandes
lancées dans l'ordre, avec des gardes qui refusent les enchaînements invalides.
Un ordonnanceur comme Airflow apporterait la reprise sur incident et la
traçabilité des exécutions, au prix d'un composant supplémentaire difficile à
justifier dans une enveloppe de 16 Go.

---

# 10. Conclusion

Le pipeline demandé est complet et fonctionne de bout en bout : une base Oracle
normalisée en troisième forme normale, une extraction dénormalisée en JSON Lines
produite par Oracle lui-même, un modèle Cassandra construit à partir des requêtes
plutôt que des entités, un formatage Spark vers Parquet partitionné avec des
analyses en Python, et une indexation Elasticsearch exposée dans un tableau de
bord Kibana de huit panneaux, le tout dans 16 Go grâce à une exécution
strictement séquentielle. Sur l'exécution de référence, les 149 300 lignes de
commande se retrouvent identiques aux quatre étapes, l'écart entre commandes
générées et documents extraits est intégralement expliqué, et le chiffre
d'affaires calculé par Spark est retrouvé au centime par Elasticsearch.

Ce que je retiens surtout, c'est que la contrainte des 16 Go m'a aidé à concevoir
le projet. Comme je ne pouvais pas avoir deux bases allumées en même temps, j'ai
dû définir précisément ce qui passe d'une phase à l'autre. Le fichier JSON n'est
donc pas une formalité imposée par l'énoncé, c'est ce qui permet à Oracle et
Cassandra de ne jamais se parler directement.

La seconde chose que je retiens, c'est qu'aucun des quatre modèles n'est meilleur
que les autres. Chacun est adapté à un usage, et le vrai travail a été de
comprendre ce que chaque passage fait gagner et ce qu'il fait perdre.

---

# 11. Annexe — enchaînement des commandes

```bash
cp .env.example .env
make install

make phase1 && make oracle-down    # Oracle    → data/json/
make phase2                        # Cassandra ← data/json/
make phase3                        # Spark lit Cassandra → data/parquet/
make cassandra-down
make phase4                        # Elasticsearch → index + tableau de bord

make test                          # cohérence globale des quatre phases
make tracer CMD=46463              # contrôle complémentaire, une commande
```

Kibana est ensuite accessible sur le port 5601. Depuis un Codespace, il faut
passer par l'onglet **PORTS** de VS Code : l'adresse `localhost` ne fonctionne
que depuis la machine distante elle-même.

La documentation technique détaillée, phase par phase, se trouve dans le dossier
`docs/` de l'archive.
