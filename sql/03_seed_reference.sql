-- ---------------------------------------------------------------------------
-- Donnees de reference.
--
-- Ces trois tables ont un contenu ferme, stable et de faible cardinalite : il
-- est ecrit en SQL, versionne avec le schema. Les donnees de masse (produits,
-- clients, adresses, commandes, lignes) sont a l'inverse produites par le
-- generateur Python, qui seul permet d'atteindre la volumetrie voulue.
-- ---------------------------------------------------------------------------

INSERT ALL
  INTO countries (country_code, country_name, region) VALUES ('FR', 'France', 'Europe de l''Ouest')
  INTO countries (country_code, country_name, region) VALUES ('BE', 'Belgique', 'Europe de l''Ouest')
  INTO countries (country_code, country_name, region) VALUES ('CH', 'Suisse', 'Europe de l''Ouest')
  INTO countries (country_code, country_name, region) VALUES ('LU', 'Luxembourg', 'Europe de l''Ouest')
  INTO countries (country_code, country_name, region) VALUES ('DE', 'Allemagne', 'Europe centrale')
  INTO countries (country_code, country_name, region) VALUES ('NL', 'Pays-Bas', 'Europe centrale')
  INTO countries (country_code, country_name, region) VALUES ('ES', 'Espagne', 'Europe du Sud')
  INTO countries (country_code, country_name, region) VALUES ('IT', 'Italie', 'Europe du Sud')
SELECT * FROM dual
/

INSERT ALL
  INTO payment_methods (payment_method_id, method_code, method_label) VALUES (1, 'CARD', 'Carte bancaire')
  INTO payment_methods (payment_method_id, method_code, method_label) VALUES (2, 'PAYPAL', 'PayPal')
  INTO payment_methods (payment_method_id, method_code, method_label) VALUES (3, 'TRANSFER', 'Virement bancaire')
  INTO payment_methods (payment_method_id, method_code, method_label) VALUES (4, 'GIFTCARD', 'Carte cadeau')
  INTO payment_methods (payment_method_id, method_code, method_label) VALUES (5, 'INSTALMENT', 'Paiement en 3 fois')
SELECT * FROM dual
/

-- Niveau 1 : les rayons (parent_category_id NULL).
INSERT ALL
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (1, 'INFORMATIQUE', 'Informatique', NULL)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (2, 'IMAGE_SON', 'Image & Son', NULL)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (3, 'TELEPHONIE', 'Téléphonie', NULL)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (4, 'MAISON', 'Maison & Cuisine', NULL)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (5, 'MODE', 'Mode', NULL)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (6, 'SPORT', 'Sport & Loisirs', NULL)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (7, 'BEAUTE', 'Beauté & Santé', NULL)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (8, 'LIVRES', 'Livres & Papeterie', NULL)
SELECT * FROM dual
/

-- Niveau 2 : les categories feuilles, seules a porter des produits.
INSERT ALL
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (101, 'PC_PORTABLE', 'Ordinateurs portables', 1)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (102, 'PC_BUREAU', 'Ordinateurs de bureau', 1)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (103, 'PERIPHERIQUE', 'Périphériques', 1)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (104, 'STOCKAGE', 'Stockage', 1)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (201, 'TELEVISEUR', 'Téléviseurs', 2)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (202, 'CASQUE', 'Casques audio', 2)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (203, 'ENCEINTE', 'Enceintes', 2)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (204, 'HOME_CINEMA', 'Home cinéma', 2)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (301, 'SMARTPHONE', 'Smartphones', 3)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (302, 'TABLETTE', 'Tablettes', 3)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (303, 'ACC_MOBILE', 'Accessoires mobiles', 3)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (304, 'OBJET_CONNECTE', 'Objets connectés', 3)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (401, 'PETIT_ELECTRO', 'Petit électroménager', 4)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (402, 'GROS_ELECTRO', 'Gros électroménager', 4)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (403, 'ARTS_TABLE', 'Arts de la table', 4)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (404, 'LINGE_MAISON', 'Linge de maison', 4)
SELECT * FROM dual
/

INSERT ALL
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (501, 'VET_HOMME', 'Vêtements homme', 5)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (502, 'VET_FEMME', 'Vêtements femme', 5)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (503, 'CHAUSSURES', 'Chaussures', 5)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (504, 'MAROQUINERIE', 'Maroquinerie', 5)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (601, 'FITNESS', 'Fitness', 6)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (602, 'CYCLISME', 'Cyclisme', 6)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (603, 'RANDONNEE', 'Randonnée', 6)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (604, 'SPORT_COLLECTIF', 'Sports collectifs', 6)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (701, 'SOIN_VISAGE', 'Soins visage', 7)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (702, 'PARFUM', 'Parfums', 7)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (703, 'CAPILLAIRE', 'Capillaire', 7)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (704, 'APPAREIL_SOIN', 'Appareils de soin', 7)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (801, 'ROMAN', 'Romans', 8)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (802, 'BD_MANGA', 'BD & Mangas', 8)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (803, 'SCOLAIRE', 'Scolaire', 8)
  INTO categories (category_id, category_code, category_name, parent_category_id) VALUES (804, 'FOURNITURE', 'Fournitures', 8)
SELECT * FROM dual
/

COMMIT
/
