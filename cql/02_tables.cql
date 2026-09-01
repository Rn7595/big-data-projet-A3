-- ---------------------------------------------------------------------------
-- Modele Cassandra : une table par requete.
--
-- La demarche est l'inverse exact de celle du modele relationnel. En SQL on
-- modelise les entites, puis on ecrit les requetes qu'on veut, le moteur se
-- debrouille avec des jointures. Cassandra ne sait pas joindre : on part donc
-- des requetes, et chaque requete recoit sa propre table, dont la cle primaire
-- est dessinee pour qu'une reponse tienne dans une seule partition.
--
-- La duplication des donnees entre ces tables n'est pas un defaut du modele,
-- c'est le modele. On paie du stockage, qui est bon marche, pour supprimer les
-- lectures multiples, qui sont cheres.
--
-- Rappel du fonctionnement, indispensable pour comprendre les choix qui
-- suivent :
--   * la CLE DE PARTITION est hachee, et ce hachage designe le noeud qui
--     detient la donnee. Elle determine donc la repartition sur le cluster ;
--   * les COLONNES DE CLUSTERING definissent l'ordre de tri des lignes SUR LE
--     DISQUE, a l'interieur d'une partition. Un tri obtenu par la cle de
--     clustering ne coute rien a la lecture, il est deja fait.
--
-- Les quatre requetes metier retenues :
--   Q1  l'historique des commandes d'un client, les plus recentes d'abord
--   Q2  le detail d'une commande designee par son identifiant
--   Q3  le chiffre d'affaires d'une categorie sur un mois donne
--   Q4  le catalogue d'une categorie, par ordre alphabetique
-- ---------------------------------------------------------------------------

-- Type utilisateur representant une ligne de commande.
--
-- C'est le coeur de la denormalisation : la relation 1-N entre ORDERS et
-- ORDER_ITEMS, qui exigeait une table separee et une jointure en SQL, devient
-- une collection imbriquee dans la ligne de commande. Le produit, sa marque et
-- sa categorie sont recopies ici : en SQL il aurait fallu deux jointures de
-- plus pour les obtenir.
--
-- Les elements d'une collection doivent etre "frozen" : Cassandra les serialise
-- alors comme une valeur unique et immuable. La consequence est a assumer :
-- modifier une ligne de commande impose de reecrire tout le tableau. C'est
-- acceptable ici parce qu'une commande passee ne change plus. Ce serait un
-- mauvais choix pour une donnee mise a jour ligne par ligne.
CREATE TYPE IF NOT EXISTS order_item (
  line_no              int,
  product_id           int,
  sku                  text,
  product_name         text,
  brand                text,
  category_id          int,
  category_name        text,
  parent_category_id   int,
  parent_category_name text,
  quantity             int,
  unit_price           decimal,
  discount_pct         decimal,
  line_amount          decimal
);

-- Q1 : "les commandes du client X, de la plus recente a la plus ancienne"
--
-- CLE DE PARTITION : customer_id.
--   Forte cardinalite (5 000 valeurs distinctes) : le hachage repartit les
--   partitions uniformement sur l'anneau, sans point chaud.
--   Taille bornee : meme un tres gros client depasse rarement quelques
--   centaines de commandes, soit quelques mega-octets. On reste tres loin des
--   100 Mo par partition au-dela desquels les performances se degradent.
--
-- CLUSTERING : order_date DESC, puis order_id DESC.
--   Le tri antichronologique est ecrit sur le disque. La requete "les 10
--   dernieres commandes de ce client" lit les 10 premieres lignes de la
--   partition et s'arrete. Aucun tri a l'execution, aucun balayage.
--   order_id est ajoute pour departager deux commandes de meme horodatage :
--   sans lui, la seconde ecraserait la premiere, la cle primaire devant etre
--   unique.
--
-- Contre-exemples a citer en soutenance :
--   * partitionner par order_date : toutes les commandes du jour tombent sur
--     le meme noeud, qui prend l'integralite du trafic d'ecriture ;
--   * partitionner par order_status : six valeurs distinctes, donc six
--     partitions enormes et un anneau totalement desequilibre ;
--   * garder ORDERS seul et poser un index secondaire sur customer_id : la
--     requete devient un scatter-gather interrogeant tous les noeuds.
CREATE TABLE IF NOT EXISTS orders_by_customer (
  customer_id            int,
  order_date             timestamp,
  order_id               int,
  order_ref              text,
  order_status           text,
  payment_method         text,
  payment_label          text,
  shipping_amount        decimal,
  customer_email         text,
  customer_first_name    text,
  customer_last_name     text,
  loyalty_tier           text,
  shipping_city          text,
  shipping_postal_code   text,
  shipping_country_code  text,
  shipping_country_name  text,
  shipping_region        text,
  items                  list<frozen<order_item>>,
  items_count            int,
  total_quantity         int,
  total_amount           decimal,
  PRIMARY KEY ((customer_id), order_date, order_id)
) WITH CLUSTERING ORDER BY (order_date DESC, order_id DESC);

-- Q2 : "la commande numero X"
--
-- Exactement les memes donnees que la table precedente, avec une autre cle
-- d'acces. C'est le principe "une table par requete" applique jusqu'au bout.
--
-- Pourquoi ne pas interroger orders_by_customer ? Parce qu'une lecture
-- Cassandra exige de connaitre la cle de partition. Chercher une commande par
-- son seul identifiant dans une table partitionnee par client obligerait a
-- balayer toutes les partitions.
--
-- Le cout de ce doublon est un doublement du volume et deux ecritures au lieu
-- d'une. C'est le compromis explicite du modele : le stockage est bon marche,
-- la lecture distribuee ne l'est pas.
CREATE TABLE IF NOT EXISTS order_by_id (
  order_id               int PRIMARY KEY,
  order_ref              text,
  order_date             timestamp,
  order_status           text,
  payment_method         text,
  payment_label          text,
  shipping_amount        decimal,
  customer_id            int,
  customer_email         text,
  customer_first_name    text,
  customer_last_name     text,
  loyalty_tier           text,
  shipping_city          text,
  shipping_postal_code   text,
  shipping_country_code  text,
  shipping_country_name  text,
  shipping_region        text,
  items                  list<frozen<order_item>>,
  items_count            int,
  total_quantity         int,
  total_amount           decimal
);

-- Q3 : "le chiffre d'affaires de la categorie X sur le mois M"
--
-- CLE DE PARTITION COMPOSITE : (category_id, year_month).
--   C'est le point le plus interessant du modele. Partitionner par la seule
--   category_id donnerait 32 partitions qui grossiraient indefiniment au fil
--   des mois : au bout de quelques annees, une categorie populaire depasserait
--   la limite pratique et deviendrait illisible. C'est l'anti-pattern de la
--   partition non bornee.
--
--   Ajouter le mois dans la cle de partition est un "bucketing" temporel : la
--   partition est desormais bornee par construction, puisqu'elle ne contient
--   qu'un mois. Le nombre de partitions croit avec le temps, ce qui est
--   exactement ce qu'on veut d'un systeme distribue.
--
--   Effet secondaire recherche : la requete metier porte justement sur un
--   couple (categorie, mois). Elle lit donc une partition et une seule.
--
-- GRANULARITE : une ligne par ligne de commande, et non par commande. Une
-- meme commande alimente donc plusieurs partitions si elle touche plusieurs
-- categories. C'est ce qui permet d'attribuer chaque euro a sa categorie, et
-- c'est cette table que Spark lira en phase 3 comme table de faits.
CREATE TABLE IF NOT EXISTS sales_by_category_month (
  category_id            int,
  year_month             text,
  order_date             timestamp,
  order_id               int,
  line_no                int,
  category_name          text,
  parent_category_id     int,
  parent_category_name   text,
  product_id             int,
  sku                    text,
  product_name           text,
  brand                  text,
  quantity               int,
  unit_price             decimal,
  discount_pct           decimal,
  line_amount            decimal,
  customer_id            int,
  loyalty_tier           text,
  order_status           text,
  country_code           text,
  country_name           text,
  region                 text,
  city                   text,
  PRIMARY KEY ((category_id, year_month), order_date, order_id, line_no)
) WITH CLUSTERING ORDER BY (order_date DESC, order_id DESC, line_no ASC);

-- Q4 : "le catalogue de la categorie X, par ordre alphabetique"
--
-- CLUSTERING sur product_name : le tri alphabetique est ecrit sur le disque,
-- la lecture le restitue sans rien trier. product_id ferme la cle pour
-- garantir son unicite en cas d'homonymie de deux produits.
--
-- La hierarchie des categories, exprimee en SQL par une cle etrangere
-- reflexive, est aplatie en deux colonnes : Cassandra ne sait pas parcourir
-- une arborescence, et une requete recursive n'a pas d'equivalent en CQL.
CREATE TABLE IF NOT EXISTS products_by_category (
  category_id            int,
  product_name           text,
  product_id             int,
  sku                    text,
  brand                  text,
  unit_price             decimal,
  is_active              boolean,
  created_at             date,
  category_code          text,
  category_name          text,
  parent_category_id     int,
  parent_category_name   text,
  PRIMARY KEY ((category_id), product_name, product_id)
) WITH CLUSTERING ORDER BY (product_name ASC, product_id ASC);
