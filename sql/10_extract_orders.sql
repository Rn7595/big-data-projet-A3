-- ---------------------------------------------------------------------------
-- REQUETE DE DENORMALISATION -- coeur de l'etape 2 du sujet.
--
-- Elle transforme six tables relationnelles en un document JSON autonome par
-- commande. Le travail est fait par Oracle lui-meme, avec le SQL/JSON normalise
-- (JSON_OBJECT, JSON_ARRAYAGG) : Python ne fait que lire des lignes et les
-- ecrire sur disque, il ne reconstruit aucune structure. La logique de
-- transformation reste donc dans le moteur, la ou sont les donnees.
--
-- Trois choix a defendre :
--
-- 1. Aplatissement des jointures. ORDERS x CUSTOMERS x PAYMENT_METHODS x
--    ADDRESSES x COUNTRIES x ORDER_ITEMS x PRODUCTS x CATEGORIES devient un
--    seul document. Cassandra ne sait pas faire de jointure : ce qui n'est pas
--    resolu ici devrait l'etre par N lectures a l'execution.
--
-- 2. Imbrication des lignes de commande. Le tableau "items" porte la relation
--    1-N a l'interieur du document. C'est exactement la structure que Cassandra
--    stockera en list<frozen<order_item>>, dans la meme partition que
--    l'en-tete : une commande complete = une seule lecture, un seul noeud.
--
-- 3. Pre-calcul des agregats. total_amount, total_quantity et items_count sont
--    volontairement absents du schema Oracle (ils sont derivables, donc les
--    stocker violerait la 3NF). Ils sont calcules ici, une fois, a l'ecriture.
--    C'est le compromis NoSQL assume : on paie au chargement ce qu'on ne veut
--    plus payer a chaque lecture.
--
-- Le CTE item_agg agrege les lignes en un seul passage, groupees par commande.
-- On evite ainsi une sous-requete correlee qui serait re-executee pour chacune
-- des commandes.
--
-- RETURNING CLOB est indispensable : sans lui, JSON_OBJECT et JSON_ARRAYAGG
-- retournent du VARCHAR2(4000) et tronquent silencieusement les commandes
-- comportant beaucoup de lignes.
--
-- FORMAT JSON signale a JSON_OBJECT que items_json est deja du JSON : sans ce
-- mot cle, le tableau serait insere comme une chaine de caracteres echappee.
-- ---------------------------------------------------------------------------

WITH item_agg AS (
  SELECT
    oi.order_id,
    COUNT(*)            AS items_count,
    SUM(oi.quantity)    AS total_quantity,
    SUM(ROUND(oi.quantity * oi.unit_price * (1 - oi.discount_pct / 100), 2)) AS items_amount,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'line_no'              VALUE oi.line_no,
        'product_id'           VALUE oi.product_id,
        'sku'                  VALUE p.sku,
        'product_name'         VALUE p.product_name,
        'brand'                VALUE p.brand,
        'category_id'          VALUE c.category_id,
        'category_name'        VALUE c.category_name,
        'parent_category_id'   VALUE pc.category_id,
        'parent_category_name' VALUE pc.category_name,
        'quantity'             VALUE oi.quantity,
        'unit_price'           VALUE oi.unit_price,
        'discount_pct'         VALUE oi.discount_pct,
        'line_amount'          VALUE ROUND(oi.quantity * oi.unit_price * (1 - oi.discount_pct / 100), 2)
        RETURNING CLOB
      )
      ORDER BY oi.line_no
      RETURNING CLOB
    ) AS items_json
  FROM order_items oi
  JOIN products    p  ON p.product_id  = oi.product_id
  JOIN categories  c  ON c.category_id = p.category_id
  LEFT JOIN categories pc ON pc.category_id = c.parent_category_id
  GROUP BY oi.order_id
)
SELECT
  o.order_id,
  JSON_OBJECT(
    'order_id'         VALUE o.order_id,
    'order_ref'        VALUE o.order_ref,
    'order_date'       VALUE TO_CHAR(o.order_date, 'YYYY-MM-DD"T"HH24:MI:SS'),
    'order_year_month' VALUE TO_CHAR(o.order_date, 'YYYY-MM'),
    'order_status'     VALUE o.order_status,
    'payment_method'   VALUE pm.method_code,
    'payment_label'    VALUE pm.method_label,
    'shipping_amount'  VALUE o.shipping_amount,
    'customer'         VALUE JSON_OBJECT(
        'customer_id'  VALUE cu.customer_id,
        'email'        VALUE cu.email,
        'first_name'   VALUE cu.first_name,
        'last_name'    VALUE cu.last_name,
        'loyalty_tier' VALUE cu.loyalty_tier,
        'signup_date'  VALUE TO_CHAR(cu.signup_date, 'YYYY-MM-DD')
        RETURNING CLOB
      ) FORMAT JSON,
    'shipping_address' VALUE JSON_OBJECT(
        'address_id'   VALUE sa.address_id,
        'city'         VALUE sa.city,
        'postal_code'  VALUE sa.postal_code,
        'country_code' VALUE sa.country_code,
        'country_name' VALUE co.country_name,
        'region'       VALUE co.region
        RETURNING CLOB
      ) FORMAT JSON,
    'items'            VALUE ia.items_json FORMAT JSON,
    'items_count'      VALUE ia.items_count,
    'total_quantity'   VALUE ia.total_quantity,
    'total_amount'     VALUE ROUND(ia.items_amount + o.shipping_amount, 2)
    RETURNING CLOB
  ) AS doc
FROM orders o
JOIN customers       cu ON cu.customer_id       = o.customer_id
JOIN payment_methods pm ON pm.payment_method_id = o.payment_method_id
JOIN addresses       sa ON sa.address_id        = o.shipping_address_id
JOIN countries       co ON co.country_code      = sa.country_code
JOIN item_agg        ia ON ia.order_id          = o.order_id
ORDER BY o.order_id
