-- ---------------------------------------------------------------------------
-- Nettoyage des donnees (etape "data cleaning" du sujet).
--
-- Les contraintes du schema (PK, FK, CHECK, UNIQUE) rejettent les erreurs
-- structurelles a l'insertion. Elles sont en revanche aveugles a tout ce qui
-- est syntaxiquement valide mais semantiquement sale : casse incoherente,
-- espaces parasites, libelles a double espace. C'est exactement ce que traite
-- ce fichier, avant l'extraction JSON.
--
-- Le generateur injecte volontairement ces anomalies sur environ 1 % des
-- lignes, afin que l'etape de nettoyage ait un effet mesurable et
-- demontrable, et non un simple passage a vide.
--
-- Chaque instruction est nommee par un marqueur "-- @name:" exploite par le
-- lanceur Python, qui journalise le nombre de lignes corrigees par regle.
--
-- Note : normaliser l'email en minuscules ne peut pas creer de doublon, la
-- contrainte UNIQUE portant deja sur des adresses distinctes hors casse.
-- ---------------------------------------------------------------------------

-- @name: emails_normalises
-- @desc: passage en minuscules et suppression des espaces de bordure
UPDATE customers
SET    email = LOWER(TRIM(email))
WHERE  email <> LOWER(TRIM(email))
/

-- @name: noms_clients_normalises
-- @desc: suppression des espaces de bordure sur les noms et prenoms
UPDATE customers
SET    first_name = TRIM(first_name),
       last_name  = TRIM(last_name)
WHERE  first_name <> TRIM(first_name)
   OR  last_name  <> TRIM(last_name)
/

-- @name: libelles_produits_normalises
-- @desc: reduction des espaces multiples et suppression des espaces de bordure
UPDATE products
SET    product_name = TRIM(REGEXP_REPLACE(product_name, ' {2,}', ' '))
WHERE  product_name <> TRIM(REGEXP_REPLACE(product_name, ' {2,}', ' '))
/

-- @name: villes_normalisees
-- @desc: meme traitement sur les villes, qui alimenteront les agregats Kibana
UPDATE addresses
SET    city = TRIM(REGEXP_REPLACE(city, ' {2,}', ' '))
WHERE  city <> TRIM(REGEXP_REPLACE(city, ' {2,}', ' '))
/

-- @name: codes_postaux_normalises
-- @desc: suppression des espaces internes des codes postaux
UPDATE addresses
SET    postal_code = REPLACE(TRIM(postal_code), ' ', '')
WHERE  postal_code <> REPLACE(TRIM(postal_code), ' ', '')
/
