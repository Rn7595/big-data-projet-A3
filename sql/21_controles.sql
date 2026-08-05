-- ---------------------------------------------------------------------------
-- Controles de qualite executes apres le nettoyage et avant l'extraction.
--
-- Deux familles :
--   * les controles BLOQUANTS doivent renvoyer zero ligne. Une ligne renvoyee
--     signale une incoherence qui se propagerait jusqu'a Kibana, ou l'on ne
--     saurait plus l'expliquer. Le pipeline s'arrete.
--   * les controles INFORMATIFS comptent des situations legitimes mais qui
--     changent les volumetries d'une phase a l'autre. Les documenter evite
--     d'avoir a justifier un ecart de comptage pendant la soutenance.
--
-- Interet des controles 3 et 4 : ils expriment des regles metier qu'aucune
-- contrainte declarative ne peut porter. Une cle etrangere garantit que
-- l'adresse de livraison existe, pas qu'elle appartient au client de la
-- commande. Ce sont les erreurs que seul un controle explicite attrape.
-- ---------------------------------------------------------------------------

-- @name: emails_non_normalises
-- @severity: blocking
-- @desc: aucun email ne doit subsister avec des espaces ou des majuscules
SELECT customer_id, email
FROM   customers
WHERE  email <> LOWER(TRIM(email))
/

-- @name: emails_dupliques
-- @severity: blocking
-- @desc: unicite de l'email independamment de la casse
SELECT LOWER(email) AS email, COUNT(*) AS nb
FROM   customers
GROUP  BY LOWER(email)
HAVING COUNT(*) > 1
/

-- @name: adresse_livraison_etrangere_au_client
-- @severity: blocking
-- @desc: l'adresse de livraison doit appartenir au client de la commande
SELECT o.order_id, o.customer_id, a.customer_id AS address_owner
FROM   orders o
JOIN   addresses a ON a.address_id = o.shipping_address_id
WHERE  a.customer_id <> o.customer_id
/

-- @name: commande_anterieure_a_inscription
-- @severity: blocking
-- @desc: une commande ne peut pas preceder l'inscription de son client
SELECT o.order_id, o.order_date, c.signup_date
FROM   orders o
JOIN   customers c ON c.customer_id = o.customer_id
WHERE  o.order_date < c.signup_date
/

-- @name: lignes_montant_invalide
-- @severity: blocking
-- @desc: aucun montant de ligne nul ou negatif apres application de la remise
SELECT order_id, line_no, quantity, unit_price, discount_pct
FROM   order_items
WHERE  ROUND(quantity * unit_price * (1 - discount_pct / 100), 2) <= 0
/

-- @name: produits_hors_categorie_feuille
-- @severity: blocking
-- @desc: un produit doit etre rattache a une categorie feuille, jamais a un rayon
SELECT p.product_id, p.category_id
FROM   products p
JOIN   categories c ON c.category_id = p.category_id
WHERE  c.parent_category_id IS NULL
/

-- @name: commandes_sans_ligne
-- @severity: info
-- @desc: paniers enregistres sans ligne ; exclus par la jointure interne de l'extraction
SELECT o.order_id, o.order_status
FROM   orders o
WHERE  NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.order_id)
/

-- @name: clients_sans_commande
-- @severity: info
-- @desc: clients inscrits n'ayant jamais commande ; absents des documents JSON
SELECT c.customer_id
FROM   customers c
WHERE  NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)
/
