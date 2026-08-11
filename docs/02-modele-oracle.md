# Phase 1 — Modele relationnel Oracle

## Modele logique

```
COUNTRIES(country_code, country_name, region)
    ▲
    │
ADDRESSES(address_id, #customer_id, address_type, street, city, postal_code, #country_code)
    ▲                     ▲
    │                     │
    │                 CUSTOMERS(customer_id, email, first_name, last_name,
    │                           birth_date, signup_date, loyalty_tier)
    │                     ▲
    │                     │
ORDERS(order_id, order_ref, #customer_id, order_date, order_status,
       #payment_method_id, #shipping_address_id, #billing_address_id, shipping_amount)
    ▲                          ▲
    │                          │
    │                  PAYMENT_METHODS(payment_method_id, method_code, method_label)
    │
ORDER_ITEMS(#order_id, line_no, #product_id, quantity, unit_price, discount_pct)
                            ▲
                            │
                     PRODUCTS(product_id, sku, product_name, brand,
                              #category_id, unit_price, is_active, created_at)
                            ▲
                            │
                     CATEGORIES(category_id, category_code, category_name,
                                #parent_category_id)   ← FK reflexive
```

Huit tables, cle primaire soulignee par convention, `#` pour les cles
etrangeres. Fichier : `sql/01_schema.sql`.

## Justification de la normalisation

**1NF** — aucun attribut multivalue. Un client peut avoir plusieurs adresses :
elles sont dans une table dediee, pas dans trois colonnes `adresse_1`,
`adresse_2`, `adresse_3`. Une commande peut porter plusieurs produits : c'est
`ORDER_ITEMS`.

**2NF** — `ORDER_ITEMS` a une cle composite `(order_id, line_no)`. Tous ses
attributs dependent de la ligne entiere : la quantite et le prix n'ont de sens
que pour cette ligne de cette commande. Aucun attribut ne depend du seul
`order_id` (il serait alors dans `ORDERS`).

**3NF** — c'est la raison d'etre de `COUNTRIES` et `PAYMENT_METHODS`. Si
`ADDRESSES` portait une colonne `country_name`, on aurait la dependance
transitive `address_id → country_code → country_name` : le libelle « France »
serait recopie des milliers de fois, et le renommer demanderait de parcourir la
table. Un referentiel de 8 lignes supprime le probleme.

### Trois choix qu'un jury peut contester

**Le montant total n'est pas stocke dans `ORDERS`.** Il se deduit des lignes.
Le stocker serait une redondance calculable, donc une violation, et surtout un
risque d'incoherence si une ligne est modifiee sans recalcul. C'est un point
essentiel pour la suite : ce total, on le calculera **une fois** au moment de
la denormalisation vers Cassandra. Le contraste entre les deux modeles tient
tout entier dans cette phrase.

**`unit_price` apparait dans `PRODUCTS` et dans `ORDER_ITEMS`.** Ce n'est pas
une redondance mais une **historisation**. `PRODUCTS.unit_price` est le prix
courant du catalogue ; `ORDER_ITEMS.unit_price` est le prix effectivement
facture ce jour-la. Les deux sont des faits differents : si le catalogue change
demain, les factures d'hier ne doivent pas bouger. Sans cette colonne, le
chiffre d'affaires historique serait faux.

**`order_status` est une colonne contrainte par CHECK, pas une table.** Le
domaine est ferme (six valeurs connues a la conception) et ne porte aucun
attribut propre. Une table de reference n'apporterait qu'une jointure de plus.
`COUNTRIES` et `PAYMENT_METHODS`, eux, portent bien des attributs (libelle,
region), d'ou la difference de traitement.

## Index

Fichier : `sql/02_indexes.sql`.

Oracle **n'indexe pas** automatiquement les cles etrangeres — contrairement a
MySQL. Deux consequences : toute modification d'une ligne mere pose un verrou
sur la table fille, et les jointures degenerent en balayage complet. Les six
index de cles etrangeres sont donc indispensables a la requete de
denormalisation, qui joint six tables sur 60 000 commandes.

L'index composite `ix_orders_customer_date (customer_id, order_date DESC)` est
volontairement l'exact miroir de la future table Cassandra
`orders_by_customer` : meme cle d'acces, meme ordre de tri. La difference est
que Cassandra ecrira physiquement les lignes dans cet ordre, alors qu'Oracle
doit encore remonter de l'index vers la table pour lire les colonnes.

`ORDER_ITEMS(order_id)` n'est pas indexe separement : la colonne est deja le
prefixe de la cle primaire composite.

## Jeu de donnees

Genere par `pipeline/phase1_oracle/generate_data.py`, volumetrie pilotee par
`.env` :

| Table | Volume par defaut |
|-------|-------------------|
| `countries` / `payment_methods` / `categories` | 8 / 5 / 40 |
| `products` | 800 |
| `customers` | 5 000 |
| `addresses` | ~11 200 (1 facturation + 1 ou 2 livraisons par client) |
| `orders` | 60 000 |
| `order_items` | ~149 000 |

Volumes releves lors d'une execution reelle. L'extraction produit **59 701**
documents et non 60 000 : les 299 commandes sans ligne sont ecartees par la
jointure interne sur les lignes de commande. L'ecart est mesure et documente
par le controle informatif `commandes_sans_ligne`, precisement pour qu'il n'y
ait rien a improviser si le jury le remarque.

**Pourquoi generer plutot que telecharger ?** Les jeux publics d'e-commerce
sont presque toujours livres a plat, deja denormalises — les charger reviendrait
a sauter l'etape que le sujet demande de realiser. La generation garantit en
outre que les regles qu'aucune contrainte declarative ne peut exprimer sont
respectees (voir plus bas), et la graine aleatoire rend le jeu reproductible,
ce qui permet de comparer les comptages d'une phase a l'autre.

**Le realisme n'est pas cosmetique.** Trois proprietes sont injectees
volontairement :

- **saisonnalite mensuelle** (pic de novembre-decembre a ×2, creux d'aout) :
  sans elle, la courbe temporelle de Kibana serait plate ;
- **loi de Pareto sur les produits et sur les clients** : environ 20 % des
  produits concentrent 70 % des lignes. C'est ce desequilibre qui donne du sens
  a la segmentation RFM de la phase 3 et qui rend visible, en phase 2, le fait
  que les partitions Cassandra ne sont pas de tailles egales ;
- **profil horaire** (pic en soiree) : rend l'histogramme horaire exploitable.

## Nettoyage et controles

Fichiers : `sql/20_nettoyage.sql`, `sql/21_controles.sql`, pilotes par
`pipeline/phase1_oracle/data_quality.py`.

Les contraintes du schema rejettent les erreurs **structurelles** a
l'insertion. Elles sont aveugles a tout ce qui est syntaxiquement valide mais
semantiquement sale. Le generateur injecte donc environ 1 % d'anomalies de ce
type — casse incoherente, espaces parasites, doubles espaces — pour que
l'etape de nettoyage ait un effet mesurable et non un passage a vide.

Cinq regles de normalisation, puis huit controles dont six **bloquants** : si
l'un d'eux remonte une ligne, le pipeline s'arrete avant l'extraction.

Deux controles meritent d'etre cites en soutenance, parce qu'ils expriment des
regles qu'**aucune contrainte declarative ne peut porter** :

- `adresse_livraison_etrangere_au_client` : une cle etrangere garantit que
  l'adresse de livraison existe, pas qu'elle appartient au client de la
  commande ;
- `commande_anterieure_a_inscription` : une commande ne peut pas preceder
  l'inscription de son client. Aucun CHECK ne peut comparer deux colonnes de
  deux tables differentes.

Le nettoyage est place **avant** l'extraction, delibérement : une donnee sale
qui franchit la frontiere JSON est recopiee dans Cassandra, puis dans Parquet,
puis dans Elasticsearch, ou plus rien ne permet de la rattacher a son origine.
On corrige a la source, une fois.

## La requete de denormalisation

Fichier : `sql/10_extract_orders.sql`. C'est le livrable central de l'etape 2
du sujet, detaille dans `docs/03-modele-cassandra.md`. Trois points a retenir :

1. **la transformation est faite par Oracle**, en SQL/JSON normalise
   (`JSON_OBJECT`, `JSON_ARRAYAGG`). Python ne fait que lire des lignes et les
   ecrire sur disque — il ne reconstruit aucune structure. La logique reste ou
   sont les donnees ;
2. **`RETURNING CLOB` est obligatoire** : sans lui, `JSON_OBJECT` renvoie du
   `VARCHAR2(4000)` et tronque silencieusement les commandes a nombreuses
   lignes. C'est le piege classique de SQL/JSON sous Oracle ;
3. **`FORMAT JSON`** signale que le tableau `items` est deja du JSON : sans ce
   mot cle, il serait insere comme une chaine echappee.

Le format de sortie est du **JSON Lines** (un document par ligne) et non un
tableau JSON unique : il se lit en flux, se decoupe trivialement — la ligne est
l'unite, c'est ce que Spark lit nativement en parallele — et resiste a
l'interruption.

## Commandes

```bash
make phase1        # ou : ./scripts/phase1_oracle.sh
make oracle-down   # avant de passer a la phase 2
```

Detail des etapes :

```bash
python -m pipeline.phase1_oracle.init_schema      # drop, tables, index, referentiels
python -m pipeline.phase1_oracle.generate_data    # jeu de donnees
python -m pipeline.phase1_oracle.data_quality     # nettoyage puis controles
python -m pipeline.phase1_oracle.extract_to_json  # data/json/*.jsonl
```
