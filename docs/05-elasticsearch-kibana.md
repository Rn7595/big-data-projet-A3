# Phase 4 — Indexation Elasticsearch et tableau de bord Kibana

## Ce que fait cette phase

Parquet est excellent pour calculer, mauvais pour chercher : repondre a « les
commandes de ce client en novembre » impose de lire les fichiers concernes.
Elasticsearch inverse le compromis — il indexe chaque champ pour repondre en
quelques millisecondes a des filtres et des agregations arbitraires, sur des
donnees que l'utilisateur ne connait pas a l'avance.

C'est la derniere marche du pipeline : passer d'un format d'analyse a un format
d'exploration, celui que Kibana sait interroger.

## Deux index, et le grain retenu

| Index | Grain | Documents |
|---|---|---|
| `ecom-order-items` | la ligne de commande | ~149 000 |
| `ecom-customers` | le client | ~3 800 |

**Pourquoi indexer le grain le plus fin plutot que les agregats ?** Parce
qu'Elasticsearch sait agreger lui-meme. En indexant la ligne de commande,
Kibana peut construire n'importe quel regroupement : par mois, par marque, par
pays, par heure, par combinaison des quatre. Un index deja agrege par mois ne
repondrait qu'aux questions prevues d'avance, et il faudrait reindexer a chaque
nouvelle question.

Les agregats calcules en phase 3 ne sont **volontairement pas** indexes : ils
feraient double emploi et introduiraient un risque d'incoherence entre deux
sources du meme chiffre. Ils restent en Parquet, ou ils servent de reference
pour verifier les totaux affiches par Kibana.

## Les mappings explicites

Fichiers : `pipeline/phase4_elastic/mappings/*.json`.

Principe retenu : **le mapping est declare, jamais devine.**

En mapping dynamique, Elasticsearch aurait typé :

- les identifiants numeriques en `long` — inutilement large ;
- les libelles en `text` analysé, donc **inutilisables comme critere de
  regroupement** : « Smartphone Nexora X14 » deviendrait trois termes separes,
  et le graphique des meilleures ventes afficherait « smartphone », « nexora »,
  « x14 » comme trois produits distincts.

D'ou les choix :

| Type retenu | Champs | Raison |
|---|---|---|
| `keyword` | `brand`, `category_name`, `country_name`, `order_status`, `segment` | valeur atomique, non analysee : c'est ce qui rend le regroupement exact possible |
| `text` + sous-champ `keyword` | `product_name` | le texte pour la recherche plein texte, le sous-champ pour l'agregation. Les deux usages sur un seul champ |
| `scaled_float` (facteur 100) | tous les montants | stocke un entier de centimes en interne : precision exacte au centime et empreinte reduite par rapport a un `double` |
| `byte` / `short` | `order_month`, `line_no`, scores RFM | le domaine est connu et petit |
| `date` | `order_date`, `derniere_commande` | permet les histogrammes temporels de Kibana |

**`dynamic: strict`** : l'indexation d'un champ non declare echoue au lieu
d'etre acceptee silencieusement. Une colonne ajoutee en amont sans mise a jour
du mapping se voit immediatement, plutot que d'apparaitre trois semaines plus
tard sous un type aberrant.

**`number_of_replicas: 0`** : sur un cluster mono-noeud, une replique ne peut
etre allouee nulle part et le cluster resterait indefiniment en etat `yellow`.
Avec zero replique, il est `green`. En production, ce serait 1 replique
minimum.

## L'identifiant des documents

```python
"_id": f"{order_id}-{line_no}"
```

L'identifiant est derive des cles metier plutot que genere par Elasticsearch.
Consequence : **reindexer met a jour les documents existants au lieu d'en creer
des doublons**. L'operation est donc rejouable sans nettoyage prealable.

## Les controles

Le script ne se contente pas d'indexer :

1. il compare le nombre de documents envoyes au nombre de documents presents
   dans l'index apres `refresh` (l'indexation etant asynchrone par defaut, sans
   ce rafraichissement le comptage porterait sur un index encore partiellement
   invisible) ;
2. il calcule le chiffre d'affaires total par une agregation Elasticsearch, a
   comparer avec celui produit par Spark en phase 3. **Les deux doivent
   coincider** : c'est le controle de bout en bout de tout le pipeline, d'Oracle
   jusqu'a Kibana.

## Le tableau de bord

Fichier : `pipeline/phase4_elastic/dashboard.py`.

**Le dashboard est genere par code, pas dessine a la souris.** Un tableau de
bord construit dans l'interface vit dans la base interne de Kibana et disparait
avec le conteneur. Ici il est decrit dans le depot, versionne, et reconstruit a
l'identique par une commande — meme argument que pour le reste du pipeline.

Huit panneaux, qui repondent aux questions d'un responsable e-commerce :

| Panneau | Type | Question |
|---|---|---|
| Chiffre d'affaires net | metrique | combien ai-je vendu ? |
| Commandes facturees | metrique | sur combien de commandes ? |
| CA par mois | barres | quelle saisonnalite ? |
| Repartition par rayon | anneau | quelle part pour chaque univers ? |
| Meilleures ventes | table | quels produits portent le CA ? |
| CA par pays | barres | ou sont mes clients ? |
| Statuts de commande | barres | quel taux d'annulation ? |
| Segmentation RFM | barres | a qui ai-je affaire ? |

Deux details de conception :

- **la vue de donnees des clients n'a pas de champ temporel.** La segmentation
  est un etat a l'instant du calcul, pas une serie temporelle. Lui en donner un
  soumettrait le panneau au selecteur de periode du tableau de bord et le
  viderait des que l'utilisateur restreint la fenetre ;
- **`timeRestore` est actif** : la periode couvrant l'historique complet est
  enregistree avec le tableau de bord, qui s'ouvre donc directement sur des
  donnees, sans reglage manuel. Un dashboard qui s'ouvre vide parce que la
  periode par defaut est « les 15 dernieres minutes » est l'accident classique
  d'une demonstration Kibana.

### Si l'import echoue

Le schema des objets Kibana varie d'une version a l'autre. En cas de refus, le
script affiche l'erreur et s'arrete **apres** avoir cree les vues de donnees :
le tableau de bord peut alors etre construit a la main dans l'interface, puis
renvoye dans le depot par

```bash
python -m pipeline.phase4_elastic.dashboard export
```

Le travail fait a la souris redevient ainsi versionne et reproductible.

## Le reglage systeme obligatoire

Elasticsearch refuse de demarrer si `vm.max_map_count` est inferieur a 262 144,
et le defaut d'un Codespace est tres en dessous. L'echec se produit **apres** le
demarrage du conteneur : sans ce reglage, on ne voit qu'un conteneur qui
s'arrete tout seul, sans explication visible.

```bash
sudo sysctl -w vm.max_map_count=262144
```

Le script de phase applique le reglage automatiquement.

## Acceder a Kibana depuis un Codespace

Le conteneur ecoute sur le port 5601 de la machine distante. Dans VS Code,
onglet **PORTS**, cliquer sur l'adresse du port 5601. L'adresse `localhost`
ne fonctionne que depuis la machine distante elle-meme.

## Commandes

```bash
make phase4
python -m pipeline.phase4_elastic.dashboard export   # sauvegarder ses retouches
```
