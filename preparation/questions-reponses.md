# Fiche de defense orale -- hors livrable

Ce fichier ne fait pas partie de l'archive remise. Il sert a une seule chose :
verifier que chaque idee du rapport peut etre expliquee avec tes mots et
montree dans le projet.

**Mode d'emploi.** Lis la question, reponds a voix haute sans regarder, puis
compare. Si une reponse ne vient pas naturellement, note-la : le passage
correspondant du rapport doit etre simplifie ou retire. Un rapport plus court
que tu maitrises vaut mieux qu'un rapport complet que tu subis.

Les chiffres cites sont ceux de l'execution du 1er septembre. Ils changent a
chaque regeneration : verifie avec `make test` avant l'oral.


## Le projet en general

**1. Resume ton projet en trois phrases.**
Je pars d'une base Oracle normalisee qui simule un site de vente en ligne. Je
denormalise ces donnees en documents JSON, un par commande, que je charge dans
Cassandra. Ensuite Spark lit Cassandra, calcule des agregats et ecrit du
Parquet, que j'indexe dans Elasticsearch pour afficher un tableau de bord
Kibana.

**2. Pourquoi quatre technologies differentes ?**
Parce que le sujet les impose, mais surtout parce qu'elles ne servent pas a la
meme chose. Oracle garantit la coherence quand on ecrit. Cassandra lit vite
mais seulement sur les questions prevues. Parquet sert aux analyses sur tout le
jeu de donnees. Elasticsearch sert a chercher et filtrer librement.

**3. Pourquoi executer les phases une par une ?**
Parce que je travaille dans un Codespace de 16 Go. Si j'allume Oracle,
Cassandra et Elasticsearch en meme temps, les conteneurs se font tuer par le
systeme. Les quatre `mem_limit` du `docker-compose.yml` totalisent 12 Go a eux
seuls, sans compter Spark et Python.

**4. Qu'est-ce qui t'a pris le plus de temps ?**
Trois choses : faire tourner PySpark (incompatibilite setuptools, puis Java 25
qui ne marche pas avec Spark 3.5), le nombre de fichiers Parquet produits par
l'ecriture partitionnee, et l'import du tableau de bord Kibana qui echouait en
erreur 500 sans explication.


## Phase 1 -- Oracle

**5. Combien de tables, et pourquoi celles-la ?**
Huit tables, dix cles etrangeres. `sql/01_schema.sql`. Les six tables metier
sont clients, adresses, commandes, lignes de commande, produits, categories.
S'y ajoutent deux tables de reference : pays et moyens de paiement.

**6. Pourquoi une table `COUNTRIES` separee ?**
Pour la 3NF. Si `ADDRESSES` portait une colonne `country_name`, on aurait la
dependance transitive `address_id -> country_code -> country_name` : le libelle
France serait recopie des milliers de fois. Une table de huit lignes suffit.

**7. Alors pourquoi `order_status` n'est pas une table de reference ?**
Parce qu'il ne porte aucun attribut propre : le code est le libelle. Une table
n'ajouterait qu'une jointure. `COUNTRIES` porte une region, `PAYMENT_METHODS`
porte un libelle : eux justifient une table. C'est une contrainte CHECK.

**8. Pourquoi le montant total n'est pas stocke dans `ORDERS` ?**
Parce qu'il se deduit des lignes. Le stocker creerait un risque d'incoherence
si une ligne change sans recalcul. Il est calcule une seule fois, au moment de
la denormalisation vers Cassandra.

**9. `unit_price` est dans `PRODUCTS` et dans `ORDER_ITEMS`, ce n'est pas une
redondance ?**
Non, c'est de l'historisation. `PRODUCTS.unit_price` est le prix du catalogue
aujourd'hui, `ORDER_ITEMS.unit_price` est le prix facture ce jour-la. Si le
catalogue change demain, les anciennes factures ne doivent pas bouger.

**10. Pourquoi generer les donnees plutot que telecharger un jeu public ?**
Parce que les jeux publics d'e-commerce sont livres a plat, deja denormalises :
les charger reviendrait a sauter l'etape que le sujet demande. La generation me
permet aussi d'injecter des anomalies pour que le nettoyage serve a quelque
chose, et la graine `SEED=42` rend le jeu reproductible.

**11. Montre-moi le nettoyage.**
`sql/20_nettoyage.sql` : cinq regles (emails en minuscules, noms, libelles
produits, villes, codes postaux). `sql/21_controles.sql` : huit controles dont
six bloquants. Le pilote est `pipeline/phase1_oracle/data_quality.py`.
441 anomalies corrigees a la derniere execution.

**12. Un exemple de regle qu'une contrainte SQL ne peut pas exprimer ?**
`commande_anterieure_a_inscription` : une commande ne peut pas preceder
l'inscription de son client. Aucun CHECK ne compare deux colonnes de deux
tables differentes. Meme chose pour `adresse_livraison_etrangere_au_client` :
la cle etrangere garantit que l'adresse existe, pas qu'elle appartient au bon
client.

**13. Qui fait la transformation en JSON, Python ou Oracle ?**
Oracle, avec `JSON_OBJECT` et `JSON_ARRAYAGG` dans `sql/10_extract_orders.sql`.
Python lit des lignes et les ecrit sur disque, il ne reconstruit rien.

**14. A quoi sert `RETURNING CLOB` ?**
Sans lui, ces fonctions renvoient un `VARCHAR2` limite a 4 000 octets. Les
commandes a plusieurs lignes seraient tronquees.

**15. Pourquoi du JSON Lines et pas un tableau JSON ?**
Un document par ligne : le fichier se lit en flux sans etre charge en memoire,
il se decoupe facilement, et une interruption n'abime que la derniere ligne.
C'est aussi ce que Spark lit nativement.


## Phase 2 -- Cassandra

**16. Comment as-tu concu le modele Cassandra ?**
A l'envers d'Oracle. En SQL je modelise les entites puis j'ecris les requetes.
En Cassandra j'ecris d'abord les questions, puis une table par question. J'ai
retenu quatre questions, donc quatre tables. `cql/02_tables.cql`.

**17. Pourquoi `customer_id` comme cle de partition pour l'historique client ?**
Trois raisons : 5 000 valeurs distinctes donc les donnees se repartissent bien ;
une partition reste petite, un gros client depasse rarement quelques centaines
de commandes ; et c'est la cle que la question fournit toujours.

**18. LA question : pourquoi une cle de partition composite
`(category_id, year_month)` ?**
Si je partitionne par la seule categorie, j'ai 32 partitions qui grossissent
indefiniment au fil des mois. Au bout de quelques annees une categorie
populaire devient trop grosse : c'est la partition non bornee. En ajoutant le
mois, chaque partition ne contient qu'un mois, donc sa taille est bornee par
construction. Et la requete metier porte justement sur une categorie et un
mois : elle lit une seule partition.

**19. Pourquoi deux tables pour les memes commandes ?**
Parce qu'une lecture Cassandra doit fournir la cle de partition. Chercher une
commande par son identifiant dans une table partitionnee par client obligerait
a tout parcourir. Je paie un doublement du stockage pour ne jamais chercher
partout.

**20. Et si je te demande une cinquieme requete ?**
Il faut une cinquieme table et un rechargement. C'est le prix du modele : on ne
peut pas improviser une requete comme en SQL. C'est aussi pour ca que Spark
existe dans la chaine, pour les analyses non prevues.

**21. Pourquoi `frozen` sur la liste d'articles ?**
Cassandra serialise la collection comme une valeur unique. Modifier une ligne
oblige a reecrire tout le tableau. C'est acceptable ici parce qu'une commande
passee ne change plus.

**22. Pourquoi pas de `BatchStatement` au chargement ?**
Un batch Cassandra sert a l'atomicite dans une partition, pas a la
performance. Des qu'il touche plusieurs partitions il devient plus lent que des
ecritures separees. J'utilise `execute_concurrent_with_args`, 64 requetes en
parallele. `pipeline/phase2_cassandra/load_json.py`.

**23. Qu'est-ce que tu as perdu en passant en NoSQL ?**
L'integrite referentielle : plus aucune cle etrangere. Les requetes imprevues.
La coherence automatique entre les deux copies d'une commande. Et les analyses
transverses efficaces, que Spark reprend a son compte.


## Phase 3 -- Spark et Parquet

**24. Pourquoi Spark en local et pas dans un conteneur ?**
Pour la memoire : c'est la seule phase ou une base reste allumee, et un
conteneur Spark en plus ne passerait pas. Le code serait identique sur un
cluster, il faudrait seulement changer l'URL du maitre.

**25. Quelle regle metier appliques-tu au chiffre d'affaires ?**
Seuls les statuts PAID, SHIPPED et DELIVERED comptent. Les commandes annulees,
retournees ou en attente sont exclues, ce qui represente environ 17 % des
lignes. Mais je ne les supprime pas : elles restent marquees par un booleen
`is_revenue`, pour pouvoir analyser le taux d'annulation.

**26. Pourquoi Parquet plutot que CSV ?**
Colonnaire (on ne lit que les colonnes utiles), compresse, type (le schema est
dans le fichier), et chaque fichier porte les min/max de ses colonnes, ce qui
permet d'en ignorer sans l'ouvrir. Mesure sur mon jeu : 75,8 Mo de JSON
deviennent 4,9 Mo de Parquet, facteur 15,6.

**27. Explique la segmentation RFM.**
Recence, frequence, montant. Chaque mesure est convertie en score de 1 a 5 par
quintiles, donc la segmentation ne depend pas de l'echelle des donnees. La
recence est inversee : peu de jours depuis le dernier achat = bon score, d'ou
le `6 - score`. La date de reference est la derniere commande observee, pas la
date du jour, sinon la segmentation vieillirait toute seule.

**28. Pourquoi 3 439 clients segmentes alors qu'il y en a 5 000 ?**
Parce que je ne segmente que les clients ayant genere du chiffre d'affaires. Un
client dont toutes les commandes sont annulees n'a pas de recence exploitable.

**29. Pourquoi as-tu regroupe uniquement sur `customer_id` ?**
Parce qu'au depart j'avais ajoute le pays et le niveau de fidelite. Un client
ayant commande depuis deux pays apparaissait en deux lignes, segmente sur la
moitie de ses achats. C'est un test qui l'a trouve, pas la relecture du code.


## Phase 4 -- Elasticsearch et Kibana

**30. Pourquoi indexer la ligne de commande et pas les agregats ?**
Parce qu'Elasticsearch sait agreger lui-meme. Avec le grain le plus fin, Kibana
peut regrouper par mois, marque, pays, heure ou n'importe quelle combinaison.
Un index deja agrege ne repondrait qu'aux questions prevues.

**31. Pourquoi declarer les mappings au lieu de laisser Elasticsearch deviner ?**
Pour controler les types : `keyword` pour ce qui sert aux regroupements,
`text` + sous-champ `keyword` pour le nom de produit, `scaled_float` avec
facteur 100 pour les montants, ce qui garde la precision au centime.
`dynamic: strict` fait echouer un champ non declare au lieu de l'accepter en
silence.

**32. Pourquoi `_id` derive des cles metier ?**
`order_id-line_no`. Reindexer met a jour les documents au lieu de creer des
doublons.

**33. Pourquoi zero replique ?**
Sur une seule machine, une replique ne peut etre placee nulle part et l'index
resterait en etat `yellow`.

**34. Le compteur de commandes de ton dashboard est-il exact ?**
Non, c'est `unique_count`, une estimation du nombre de valeurs distinctes. Le
graphique des statuts compte par ailleurs des lignes de commande, pas des
commandes : il ne donne pas un taux d'annulation par commande.


## Resultats et controles -- les questions qui piegent

**35. Pourquoi 59 723 documents alors que tu as 60 000 commandes ?**
Parce que 277 commandes n'ont aucune ligne. La requete d'extraction fait une
jointure interne sur les lignes, donc elle les ecarte. Le nombre 277 est mesure
independamment sur Oracle par le controle `commandes_sans_ligne`, et
60 000 - 277 = 59 723 est verifie par `make test`.

**36. Comment sais-tu que rien ne s'est perdu entre les phases ?**
Les 149 300 lignes de commande sont identiques dans Oracle, dans le JSON, dans
Cassandra, dans Parquet et dans Elasticsearch. Et le chiffre d'affaires calcule
par Spark, 5 338 199,61 EUR, est retrouve par une agregation Elasticsearch.

**37. Ton `make test` prouve-t-il que les bases sont coherentes maintenant ?**
Non, et c'est important. Il lit les rapports que les phases ont ecrits dans
`data/reports/`, il ne reinterroge pas les bases. Ses conclusions portent sur
l'execution qui a produit ces rapports. C'est pour ca que j'ai ajoute
`make tracer`, qui relit les sources pour une commande precise.

**38. Ces controles peuvent-ils passer alors que le pipeline est faux ?**
Ils verifient des egalites de volumetrie et de montant, pas chaque champ de
chaque document. Ils garantissent que la meme regle est appliquee partout, pas
que la regle soit la bonne.

**39. Un cas ou un controle a reellement servi ?**
Oui. Une fois, la phase 3 s'est interrompue et la phase 4 a reindexe les
fichiers Parquet de l'execution precedente, sans afficher d'erreur.
`make test` a detecte l'ecart : 150 028 lignes cote Oracle et Cassandra contre
149 186 cote Parquet et Elasticsearch.

**40. Quelles sont les limites de ton travail ?**
Tout tourne sur une seule machine, je n'ai teste aucun deploiement distribue.
Le pipeline est rejoue en entier a chaque fois, il n'y a pas de chargement
incremental. Les donnees sont synthetiques, donc propres par construction sauf
les anomalies que j'ai injectees volontairement. Et l'orchestration reste
manuelle : les phases s'enchainent par des commandes lancees dans l'ordre, avec
des gardes qui refusent les enchainements invalides. Un ordonnanceur apporterait
la reprise sur incident, mais c'est un composant de plus a faire tenir dans
16 Go.


## Verification finale

Coche mentalement. Si tu bloques sur une question, dis-le : on retire le
passage correspondant du rapport.

- [ ] Questions 1 a 4 : vue d'ensemble
- [ ] Questions 5 a 15 : Oracle et denormalisation
- [ ] Questions 16 a 23 : modele Cassandra
- [ ] Questions 24 a 29 : Spark, Parquet, RFM
- [ ] Questions 30 a 34 : Elasticsearch et Kibana
- [ ] Questions 35 a 40 : resultats, portee des controles, limites
