-- ---------------------------------------------------------------------------
-- Schema relationnel normalise (3NF) du domaine e-commerce.
--
-- Principes retenus :
--   * une table par entite, aucune colonne multivaluee ni repetee (1NF) ;
--   * tout attribut non cle depend de la totalite de la cle (2NF) ;
--   * aucune dependance transitive entre attributs non cles (3NF) : les
--     libelles de pays et de moyen de paiement sont sortis dans leurs propres
--     tables de reference plutot que recopies dans ADDRESSES et ORDERS ;
--   * aucun montant total n'est stocke : il se deduit des lignes de commande.
--     C'est precisement ce calcul que la phase de denormalisation vers
--     Cassandra materialisera une fois pour toutes a l'ecriture.
--
-- Les cles primaires sont des cles techniques fournies par le generateur, de
-- maniere deterministe : les memes identifiants se retrouvent a l'identique
-- dans Cassandra, dans Parquet et dans Elasticsearch, ce qui rend les
-- controles de coherence de bout en bout possibles.
-- ---------------------------------------------------------------------------

CREATE TABLE countries (
  country_code   VARCHAR2(2)   NOT NULL,
  country_name   VARCHAR2(60)  NOT NULL,
  region         VARCHAR2(30)  NOT NULL,
  CONSTRAINT pk_countries PRIMARY KEY (country_code)
)
/

COMMENT ON TABLE countries IS 'Referentiel des pays de livraison (supprime la dependance transitive code -> libelle)'
/

CREATE TABLE payment_methods (
  payment_method_id  NUMBER(2)     NOT NULL,
  method_code        VARCHAR2(20)  NOT NULL,
  method_label       VARCHAR2(60)  NOT NULL,
  CONSTRAINT pk_payment_methods PRIMARY KEY (payment_method_id),
  CONSTRAINT uq_payment_methods_code UNIQUE (method_code)
)
/

COMMENT ON TABLE payment_methods IS 'Referentiel des moyens de paiement'
/

-- Hierarchie de categories a deux niveaux, modelisee par une cle etrangere
-- reflexive : un rayon a parent_category_id NULL, une categorie feuille pointe
-- vers son rayon. Une table unique suffit et reste extensible en profondeur.
CREATE TABLE categories (
  category_id         NUMBER(6)     NOT NULL,
  category_code       VARCHAR2(30)  NOT NULL,
  category_name       VARCHAR2(80)  NOT NULL,
  parent_category_id  NUMBER(6),
  CONSTRAINT pk_categories PRIMARY KEY (category_id),
  CONSTRAINT uq_categories_code UNIQUE (category_code),
  CONSTRAINT fk_categories_parent FOREIGN KEY (parent_category_id)
             REFERENCES categories (category_id)
)
/

COMMENT ON TABLE categories IS 'Arborescence des categories produit (FK reflexive)'
/

CREATE TABLE products (
  product_id    NUMBER(8)      NOT NULL,
  sku           VARCHAR2(20)   NOT NULL,
  product_name  VARCHAR2(120)  NOT NULL,
  brand         VARCHAR2(60)   NOT NULL,
  category_id   NUMBER(6)      NOT NULL,
  unit_price    NUMBER(10,2)   NOT NULL,
  is_active     NUMBER(1)      DEFAULT 1 NOT NULL,
  created_at    DATE           NOT NULL,
  CONSTRAINT pk_products PRIMARY KEY (product_id),
  CONSTRAINT uq_products_sku UNIQUE (sku),
  CONSTRAINT fk_products_category FOREIGN KEY (category_id)
             REFERENCES categories (category_id),
  CONSTRAINT ck_products_price CHECK (unit_price > 0),
  CONSTRAINT ck_products_active CHECK (is_active IN (0, 1))
)
/

COMMENT ON TABLE products IS 'Catalogue produit ; unit_price est le prix courant du catalogue'
/

CREATE TABLE customers (
  customer_id   NUMBER(8)      NOT NULL,
  email         VARCHAR2(150)  NOT NULL,
  first_name    VARCHAR2(60)   NOT NULL,
  last_name     VARCHAR2(60)   NOT NULL,
  birth_date    DATE,
  signup_date   DATE           NOT NULL,
  loyalty_tier  VARCHAR2(10)   DEFAULT 'BRONZE' NOT NULL,
  CONSTRAINT pk_customers PRIMARY KEY (customer_id),
  CONSTRAINT uq_customers_email UNIQUE (email),
  CONSTRAINT ck_customers_tier CHECK (loyalty_tier IN ('BRONZE', 'SILVER', 'GOLD', 'PLATINUM'))
)
/

COMMENT ON TABLE customers IS 'Clients ; l''email est la cle metier, l''identifiant est technique'
/

-- Un client peut avoir plusieurs adresses : la relation 1-N interdit de loger
-- l'adresse dans CUSTOMERS sans violer la premiere forme normale.
CREATE TABLE addresses (
  address_id    NUMBER(8)     NOT NULL,
  customer_id   NUMBER(8)     NOT NULL,
  address_type  VARCHAR2(10)  NOT NULL,
  street        VARCHAR2(120) NOT NULL,
  city          VARCHAR2(80)  NOT NULL,
  postal_code   VARCHAR2(12)  NOT NULL,
  country_code  VARCHAR2(2)   NOT NULL,
  CONSTRAINT pk_addresses PRIMARY KEY (address_id),
  CONSTRAINT fk_addresses_customer FOREIGN KEY (customer_id)
             REFERENCES customers (customer_id),
  CONSTRAINT fk_addresses_country FOREIGN KEY (country_code)
             REFERENCES countries (country_code),
  CONSTRAINT ck_addresses_type CHECK (address_type IN ('BILLING', 'SHIPPING'))
)
/

COMMENT ON TABLE addresses IS 'Adresses de facturation et de livraison rattachees a un client'
/

-- Le statut reste une colonne contrainte par CHECK plutot qu'une table de
-- reference : le domaine est ferme, connu a la conception, et ne porte aucun
-- attribut propre. Une table dediee n'apporterait qu'une jointure de plus.
CREATE TABLE orders (
  order_id             NUMBER(10)    NOT NULL,
  order_ref            VARCHAR2(20)  NOT NULL,
  customer_id          NUMBER(8)     NOT NULL,
  order_date           DATE          NOT NULL,
  order_status         VARCHAR2(12)  NOT NULL,
  payment_method_id    NUMBER(2)     NOT NULL,
  shipping_address_id  NUMBER(8)     NOT NULL,
  billing_address_id   NUMBER(8)     NOT NULL,
  shipping_amount      NUMBER(8,2)   DEFAULT 0 NOT NULL,
  CONSTRAINT pk_orders PRIMARY KEY (order_id),
  CONSTRAINT uq_orders_ref UNIQUE (order_ref),
  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id)
             REFERENCES customers (customer_id),
  CONSTRAINT fk_orders_payment FOREIGN KEY (payment_method_id)
             REFERENCES payment_methods (payment_method_id),
  CONSTRAINT fk_orders_ship_addr FOREIGN KEY (shipping_address_id)
             REFERENCES addresses (address_id),
  CONSTRAINT fk_orders_bill_addr FOREIGN KEY (billing_address_id)
             REFERENCES addresses (address_id),
  CONSTRAINT ck_orders_status CHECK (order_status IN
             ('PENDING', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED')),
  CONSTRAINT ck_orders_shipping CHECK (shipping_amount >= 0)
)
/

COMMENT ON TABLE orders IS 'En-tete de commande ; le montant total n''est pas stocke car derivable'
/

-- Table associative entre ORDERS et PRODUCTS, porteuse de ses propres
-- attributs (quantite, prix). La cle primaire composite (order_id, line_no)
-- garantit l'unicite de la ligne au sein de sa commande.
--
-- unit_price est duplique depuis PRODUCTS a dessein : ce n'est pas une
-- redondance mais une historisation. Le prix facture est celui du jour de la
-- commande et doit rester stable si le catalogue evolue.
CREATE TABLE order_items (
  order_id      NUMBER(10)    NOT NULL,
  line_no       NUMBER(3)     NOT NULL,
  product_id    NUMBER(8)     NOT NULL,
  quantity      NUMBER(4)     NOT NULL,
  unit_price    NUMBER(10,2)  NOT NULL,
  discount_pct  NUMBER(5,2)   DEFAULT 0 NOT NULL,
  CONSTRAINT pk_order_items PRIMARY KEY (order_id, line_no),
  CONSTRAINT fk_order_items_order FOREIGN KEY (order_id)
             REFERENCES orders (order_id) ON DELETE CASCADE,
  CONSTRAINT fk_order_items_product FOREIGN KEY (product_id)
             REFERENCES products (product_id),
  CONSTRAINT ck_order_items_qty CHECK (quantity > 0),
  CONSTRAINT ck_order_items_price CHECK (unit_price > 0),
  CONSTRAINT ck_order_items_discount CHECK (discount_pct BETWEEN 0 AND 90)
)
/

COMMENT ON TABLE order_items IS 'Lignes de commande ; unit_price historise le prix facture'
/
