# Script de la soutenance video (10 minutes)

## Avant d'enregistrer

**Ne jouez pas le pipeline en direct.** Quatre phases prennent 25 minutes ; la
video en fait 10. Executez tout avant, puis commentez des resultats deja
presents a l'ecran. Si vous voulez montrer une execution reelle, reduisez la
volumetrie dans `.env` :

```bash
NB_ORDERS=5000
NB_CUSTOMERS=800
```

Les temps sont alors divises par dix et une phase se deroule en direct pendant
que vous parlez.

**Ou presenter : dans VS Code, pas sur GitHub.**

GitHub ne sait ni executer une commande, ni afficher Kibana. Un depot en ligne
montre du code fige ; la soutenance doit montrer une chaine qui tourne. Tout se
passe donc dans le Codespace, avec son terminal.

L'application VS Code de bureau est preferable au navigateur pour
l'enregistrement : pas de barre d'adresse ni d'onglets a l'ecran, et les
raccourcis clavier ne sont pas intercepes par le navigateur. Pour y basculer :
`Ctrl+Shift+P` puis `Open in VS Code Desktop`. La machine reste la meme, seule
la fenetre change.

GitHub peut servir dix secondes en ouverture, pour montrer que le projet est
versionne et organise. Ce n'est pas indispensable.

**Preparez vos fenetres a l'avance :**

| Fenetre | Contenu |
|---|---|
| VS Code | terminal ouvert en bas, police agrandie (`Ctrl` + `+` deux ou trois fois) |
| VS Code | onglets deja ouverts : `docker-compose.yml`, `sql/01_schema.sql`, `sql/10_extract_orders.sql`, `cql/02_tables.cql` |
| Navigateur | Kibana, dashboard charge, periode reglee |

Fermez l'explorateur de fichiers pendant les demonstrations en terminal
(`Ctrl+B`) : l'ecran gagne en lisibilite une fois compresse par la video.

**Pour enregistrer**, au choix : la barre de jeu de Windows (`Win+G`), OBS
Studio, ou l'enregistrement d'ecran de PowerPoint. Enregistrez en 1080p et
verifiez qu'un texte de terminal reste lisible sur la video finale avant de
tourner les dix minutes.

**Trois verifications juste avant :**

```bash
make status                          # aucun conteneur inutile ne tourne
make test                            # les 7 controles passent
head -1 data/json/orders.jsonl | python3 -m json.tool | head -30
```

---

## Deroule minute par minute

### 0:00 – 0:50 — Le sujet et l'architecture

*Ecran : le schema de `docs/01-architecture.md`.*

> « Le sujet demande une chaine complete : une source SQL relationnelle,
> denormalisee vers Cassandra en passant par du JSON, reformatee avec Spark en
> Parquet, puis indexee dans Elasticsearch pour un tableau de bord Kibana. J'ai
> choisi un theme e-commerce : clients, commandes, produits, categories.
>
> Contrainte que je me suis imposee : l'ensemble tourne dans un Codespace de
> 16 Go, **une phase a la fois**. Aucune brique ne tourne en meme temps qu'une
> autre. »

**La phrase qui montre que vous avez compris le probleme :**

> « C'est possible parce que deux phases ne se parlent jamais directement : le
> passage de temoin est un **fichier sur disque**. Oracle est eteint avant que
> Cassandra ne demarre. »

*Montrer `docker-compose.yml`, les `profiles:`.*

> « La sequentialite n'est pas une regle que je dois me rappeler, elle est
> portee par le fichier : `docker compose up` sans argument ne demarre rien. »

---

### 0:50 – 2:35 — Phase 1 : Oracle

*Ecran : `sql/01_schema.sql`.*

> « Huit tables en troisieme forme normale. Deux referentiels, `COUNTRIES` et
> `PAYMENT_METHODS`, existent uniquement pour supprimer des dependances
> transitives : sans eux, le libelle "France" serait recopie des milliers de
> fois. »

**Le point a marteler — il prepare toute la suite :**

> « Remarquez ce qui n'est **pas** dans la table `ORDERS` : le montant total.
> Il se deduit des lignes de commande. Le stocker violerait la 3NF. Retenez ce
> point, c'est exactement ce que la denormalisation Cassandra va materialiser. »

*Ecran : sortie de la phase 1, section nettoyage.*

> « Les contraintes du schema rejettent les erreurs structurelles. Elles sont
> aveugles a ce qui est valide mais sale : casse incoherente, espaces
> parasites. Mon generateur en injecte 1 %, le nettoyage en corrige 445.
>
> Et deux controles expriment des regles qu'**aucune contrainte declarative ne
> peut porter** : une cle etrangere garantit que l'adresse de livraison existe,
> pas qu'elle appartient au client de la commande. »

*Ecran : `sql/10_extract_orders.sql`, puis un document JSON.*

> « La denormalisation est faite par Oracle lui-meme, en SQL/JSON. Python ne
> fait que lire des lignes et les ecrire. Resultat : six tables aplaties en un
> document par commande, avec les lignes imbriquees. 59 701 documents en
> 7 secondes. »

---

### 2:35 – 4:30 — Phase 2 : Cassandra

*Ecran : `cql/02_tables.cql`.*

> « Ici la methode s'inverse. En SQL on modelise les entites et on ecrit
> n'importe quelle requete. Cassandra ne sait pas joindre : on part des
> requetes, et **une table repond a une requete**. »

**Le passage le plus technique de la video — prenez le temps :**

> « Regardez `sales_by_category_month`. La cle de partition est composite :
> categorie **et** mois.
>
> Pourquoi le mois ? Si je partitionne par la seule categorie, la partition
> grossit indefiniment au fil des annees. C'est l'anti-pattern de la partition
> non bornee. En ajoutant le mois, elle est bornee par construction, et le
> nombre de partitions croit avec le temps — ce qu'on veut d'un systeme
> distribue.
>
> Et `orders_by_customer` et `order_by_id` contiennent les memes donnees. Ce
> n'est pas une erreur : une lecture Cassandra exige la cle de partition.
> Chercher une commande par son identifiant dans une table partitionnee par
> client obligerait a balayer tout le cluster. On paie du stockage, qui est bon
> marche, pour supprimer des lectures distribuees, qui sont cheres. »

*Ecran : `python -m pipeline.phase2_cassandra.demo_queries`.*

> « Q1 : les commandes d'un client, deja triees du plus recent au plus ancien —
> l'ordre est celui du disque, aucun tri n'est calcule.
>
> Q2 : la meme commande, atteinte par une autre cle. Regardez la somme des
> lignes : elle correspond au total pre-calcule. Ce total n'existe pas dans
> Oracle. Il a ete calcule **une fois**, a l'ecriture. C'est ca, le compromis
> NoSQL. »

*Si on vous demande ce que vous avez perdu :* integrite referentielle, requetes
imprevues, unicite de la verite, agregats libres.

---

### 4:30 – 6:15 — Phase 3 : Spark et Parquet

> « Cassandra repond vite aux quatre questions pour lesquelles je l'ai modelise.
> Il ne sait pas repondre a "quel est le chiffre d'affaires par mois, tous
> rayons confondus" : un `SUM` n'est possible que dans une partition.
>
> **C'est exactement le trou que Spark comble.** »

*Ecran : `pipeline/phase3_spark/transforms.py`.*

> « Spark tourne en local, sans conteneur. Pour 149 000 lignes, un cluster
> serait du folklore. Mais le code est identique : passer sur un vrai cluster ne
> demanderait que de changer l'URL du maitre.
>
> Une regle metier a defendre : les commandes annulees ou retournees ne sont pas
> du chiffre d'affaires. Mais je ne les supprime pas — je les **qualifie**,
> avec une colonne booleenne et un montant net a zero. Je peux donc analyser le
> taux d'annulation sans rien recharger. »

*Ecran : arborescence `data/parquet/fact_order_items/order_year=2025/order_month=11/`.*

> « Parquet est colonnaire, compresse, type, et partitionne par annee et mois :
> une requete sur novembre 2025 ne lit que ce repertoire. C'est le meme principe
> que le bucketing temporel de ma cle Cassandra, applique au systeme de
> fichiers.
>
> **Le chiffre : 76 Mo de JSON deviennent 8,3 Mo de Parquet. Facteur 9.** »

---

### 6:15 – 8:30 — Phase 4 : Elasticsearch et Kibana

*Ecran : `pipeline/phase4_elastic/mappings/order_items.json`.*

> « J'indexe la ligne de commande, le grain le plus fin. Pourquoi pas les
> agregats ? Parce qu'Elasticsearch sait agreger lui-meme : au grain fin, Kibana
> construit n'importe quel regroupement. Un index pre-agrege ne repondrait
> qu'aux questions prevues d'avance.
>
> Les mappings sont **declares, jamais devines**. En mapping dynamique, "Ecouteurs
> sans fil Nexora X14" serait analyse en plusieurs termes, et mon tableau des
> meilleures ventes afficherait "ecouteurs", "nexora", "x14" comme trois produits
> distincts. D'ou le type `keyword`. »

*Ecran : le dashboard Kibana. Laissez-le respirer.*

> « Huit panneaux : chiffre d'affaires, saisonnalite — le pic de fin d'annee est
> visible —, repartition par rayon, meilleures ventes, geographie, statuts, et
> la segmentation RFM calculee par Spark.
>
> Le dashboard n'est pas dessine a la souris : il est **genere par code et
> versionne** dans le depot. Un dashboard construit dans l'interface disparait
> avec le conteneur. Celui-la se reconstruit par une commande. »

*Cliquez sur un segment du camembert pour filtrer : la reactivite est votre
meilleur argument visuel.*

---

### 8:30 – 9:30 — La preuve de bout en bout

*Ecran : `make test`.*

**Le moment le plus fort de votre soutenance. Ne le sautez pas.**

> « Comment est-ce que je sais que rien n'a ete perdu entre Oracle et Kibana ?
> Pas parce que je l'ai regarde : parce qu'un controle automatique le verifie.
>
> 149 215 lignes de commande dans Oracle, dans Cassandra, dans Parquet, dans
> Elasticsearch. Le meme nombre aux quatre etapes.
>
> L'ecart entre 60 000 commandes et 59 701 documents ? Integralement explique :
> ce sont les 299 paniers sans ligne, ecartes par la jointure interne, et
> comptes par un controle dedie.
>
> Et le dernier controle est le plus fort : Spark et Elasticsearch calculent le
> meme chiffre d'affaires, par deux chemins totalement independants. »

---

### 9:30 – 10:00 — Limites et conclusion

**Enoncez vos limites avant qu'on vous les oppose. C'est ce qui separe une
bonne soutenance d'une tres bonne.**

> « Les limites, je les connais : un seul noeud partout, donc pas de tolerance
> aux pannes — en production ce serait `NetworkTopologyStrategy` avec facteur 3
> et lectures en `LOCAL_QUORUM`. La securite d'Elasticsearch est desactivee.
> Les donnees sont generees, pas reelles.
>
> Ce que le projet demontre, c'est la chaine : chaque technologie fait ce
> qu'elle sait faire, Oracle la coherence en ecriture, Cassandra la lecture
> ciblee, Spark l'analyse transverse, Elasticsearch l'exploration — et le
> passage de l'une a l'autre est explicite, mesure et verifie. »

---

## Les cinq questions probables

| Question | Reponse en une phrase |
|---|---|
| « Pourquoi cette cle de partition ? » | Forte cardinalite pour repartir sur l'anneau, taille bornee par le bucket mensuel, et elle correspond a la question posee. |
| « Qu'avez-vous perdu en denormalisant ? » | L'integrite referentielle, les requetes imprevues, l'unicite de la verite, et les agregats transverses — c'est pour ces derniers que Spark existe dans la chaine. |
| « Pourquoi Parquet plutot que CSV ? » | Colonnaire, compresse, type, filtrable par statistiques de fichier : facteur 9 mesure sur ce jeu. |
| « Vos donnees sont-elles realistes ? » | Generees avec saisonnalite, loi de Pareto et profil horaire ; panier median de 58 euros, conforme au secteur. |
| « Pourquoi pas tout en meme temps ? » | Contrainte de 16 Go assumee, et rendue structurelle par les profils Compose — le fichier interdit de tout allumer. |

## Erreurs a eviter

- **Lire l'ecran a voix haute.** Le jury lit aussi. Expliquez *pourquoi*, pas
  *quoi*.
- **Passer trop vite sur Cassandra.** C'est le coeur du sujet et la partie la
  plus notee. Deux minutes minimum.
- **Montrer du code ligne a ligne.** Montrez un fichier, pointez trois lignes,
  passez.
- **Terminer sur le dashboard.** Terminez sur `make test` : la preuve vaut mieux
  que la jolie image.
