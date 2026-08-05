-- ---------------------------------------------------------------------------
-- Second document de sortie : le catalogue produit, aplati avec sa categorie
-- et son rayon.
--
-- Il alimente la table Cassandra products_by_category, qui repond a la requete
-- "lister le catalogue d'une categorie". La hierarchie CATEGORIES, exprimee en
-- SQL par une cle etrangere reflexive, est resolue ici en deux colonnes plates
-- (categorie et rayon) : Cassandra ne sait pas parcourir une arborescence.
-- ---------------------------------------------------------------------------

SELECT
  p.product_id,
  JSON_OBJECT(
    'product_id'           VALUE p.product_id,
    'sku'                  VALUE p.sku,
    'product_name'         VALUE p.product_name,
    'brand'                VALUE p.brand,
    'unit_price'           VALUE p.unit_price,
    'is_active'            VALUE p.is_active,
    'created_at'           VALUE TO_CHAR(p.created_at, 'YYYY-MM-DD'),
    'category_id'          VALUE c.category_id,
    'category_code'        VALUE c.category_code,
    'category_name'        VALUE c.category_name,
    'parent_category_id'   VALUE pc.category_id,
    'parent_category_name' VALUE pc.category_name
    RETURNING CLOB
  ) AS doc
FROM products p
JOIN categories c ON c.category_id = p.category_id
LEFT JOIN categories pc ON pc.category_id = c.parent_category_id
ORDER BY p.product_id
