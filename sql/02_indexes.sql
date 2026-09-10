-- ---------------------------------------------------------------------------
-- Index applicatifs.
--
-- Oracle n'indexe PAS automatiquement les cles etrangeres (contrairement a
-- MySQL). Sans index sur la colonne fille, toute modification de la ligne mere
-- pose un verrou de table sur la fille, et les jointures degenerent en balayage
-- complet. Ces index sont donc indispensables pour la requete de
-- denormalisation de la phase 1, qui joint ORDERS, ORDER_ITEMS, PRODUCTS,
-- CATEGORIES, CUSTOMERS et ADDRESSES.
--
-- ORDER_ITEMS(order_id) n'apparait pas ci-dessous : cette colonne est deja le
-- prefixe de la cle primaire composite, donc deja indexee.
-- ---------------------------------------------------------------------------

CREATE INDEX ix_products_category ON products (category_id)
/

CREATE INDEX ix_addresses_customer ON addresses (customer_id)
/

CREATE INDEX ix_orders_payment ON orders (payment_method_id)
/

CREATE INDEX ix_orders_ship_addr ON orders (shipping_address_id)
/

CREATE INDEX ix_orders_bill_addr ON orders (billing_address_id)
/

CREATE INDEX ix_order_items_product ON order_items (product_id)
/

-- Index composite couvrant la requete metier la plus frequente cote SQL :
-- "les commandes d'un client, de la plus recente a la plus ancienne".
-- C'est l'exact equivalent relationnel de la table Cassandra
-- orders_by_customer : meme cle d'acces, meme ordre de tri. La difference est
-- que Cassandra ecrit physiquement les lignes dans cet ordre, la ou Oracle
-- doit encore remonter de l'index vers la table.
CREATE INDEX ix_orders_customer_date ON orders (customer_id, order_date DESC)
/

CREATE INDEX ix_orders_date ON orders (order_date)
/
