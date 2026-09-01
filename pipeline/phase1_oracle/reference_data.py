"""Referentiels utilises par le generateur de donnees.

Ces tables de correspondance sont isolees du code de generation pour garder
`generate_data.py` lisible : on y trouve les gammes de prix par categorie, les
marques, et la geographie utilisee pour les adresses.
"""

from __future__ import annotations

# Pour chaque code de categorie feuille : les types de produits vendus, chacun
# avec sa propre fourchette de prix.
#
# La fourchette est definie par TYPE DE PRODUIT et non par categorie. Une
# fourchette unique par categorie produisait des aberrations : la categorie
# Peripheriques allant de 19 a 549 EUR, une webcam pouvait couter 500 EUR et un
# ecran 27 pouces 20 EUR. Un jury qui lit le tableau des meilleures ventes le
# remarque immediatement.
CATALOGUE: dict[str, tuple[tuple[str, float, float], ...]] = {
    "PC_PORTABLE": (("Ordinateur portable", 399, 899), ("Ultrabook", 899, 1799),
                    ("PC portable gamer", 999, 2299)),
    "PC_BUREAU": (("Ordinateur de bureau", 349, 899), ("Tour gamer", 899, 1999),
                  ("Mini PC", 249, 649)),
    "PERIPHERIQUE": (("Clavier mecanique", 39, 169), ("Souris sans fil", 15, 89),
                     ("Ecran 27 pouces", 129, 549), ("Webcam", 25, 129)),
    "STOCKAGE": (("Disque SSD", 39, 229), ("Disque dur externe", 49, 179),
                 ("Cle USB", 6, 39), ("Carte memoire", 9, 69)),
    "TELEVISEUR": (("Televiseur LED", 249, 599), ("Televiseur QLED", 599, 1499),
                   ("Televiseur OLED", 999, 2499)),
    "CASQUE": (("Ecouteurs sans fil", 19, 199), ("Casque bluetooth", 39, 249),
               ("Casque a reduction de bruit", 129, 399)),
    "ENCEINTE": (("Enceinte nomade", 29, 159), ("Enceinte connectee", 49, 249),
                 ("Barre de son", 129, 699)),
    "HOME_CINEMA": (("Videoprojecteur", 299, 1499), ("Ampli home cinema", 349, 1299),
                    ("Kit 5.1", 199, 899)),
    "SMARTPHONE": (("Telephone reconditionne", 99, 349), ("Smartphone", 199, 999),
                   ("Smartphone pliable", 899, 1799)),
    "TABLETTE": (("Liseuse", 89, 249), ("Tablette tactile", 149, 899),
                 ("Tablette graphique", 79, 1249)),
    "ACC_MOBILE": (("Verre trempe", 6, 19), ("Coque de protection", 8, 39),
                   ("Chargeur rapide", 15, 59), ("Batterie externe", 19, 89)),
    "OBJET_CONNECTE": (("Bracelet d'activite", 29, 99), ("Montre connectee", 99, 599),
                       ("Balance connectee", 29, 129)),
    "PETIT_ELECTRO": (("Friteuse sans huile", 59, 199), ("Cafetiere expresso", 79, 599),
                      ("Aspirateur balai", 129, 699), ("Robot patissier", 199, 899)),
    "GROS_ELECTRO": (("Lave-linge", 299, 899), ("Lave-vaisselle", 279, 849),
                     ("Refrigerateur combine", 349, 1299), ("Four encastrable", 249, 1099)),
    "ARTS_TABLE": (("Verres a pied", 12, 59), ("Service d'assiettes", 29, 149),
                   ("Set de couteaux", 39, 229), ("Cocotte en fonte", 49, 349)),
    "LINGE_MAISON": (("Lot de serviettes", 12, 49), ("Parure de lit", 25, 129),
                     ("Couette 4 saisons", 39, 189), ("Rideaux occultants", 19, 99)),
    "VET_HOMME": (("Jean droit", 29, 99), ("Chemise en lin", 25, 89),
                  ("Pull en laine", 35, 149), ("Veste matelassee", 59, 249)),
    "VET_FEMME": (("Blouse en soie", 29, 129), ("Jean taille haute", 29, 99),
                  ("Robe portefeuille", 35, 159), ("Manteau long", 69, 289)),
    "CHAUSSURES": (("Sandales", 19, 79), ("Baskets en cuir", 49, 179),
                   ("Bottines", 45, 199), ("Chaussures de ville", 59, 229)),
    "MAROQUINERIE": (("Ceinture en cuir", 19, 69), ("Portefeuille", 19, 99),
                     ("Sac a dos urbain", 35, 159), ("Sac a main", 45, 429)),
    "FITNESS": (("Tapis de yoga", 12, 49), ("Halteres reglables", 39, 249),
                ("Banc de musculation", 79, 399), ("Rameur pliable", 199, 899)),
    "CYCLISME": (("Casque de velo", 25, 129), ("Compteur GPS", 39, 349),
                 ("VTT tout suspendu", 399, 1899), ("Velo de route", 449, 2399)),
    "RANDONNEE": (("Batons telescopiques", 19, 79), ("Sac de randonnee", 39, 179),
                  ("Chaussures de trek", 59, 219), ("Tente 2 places", 69, 499)),
    "SPORT_COLLECTIF": (("Ballon de football", 9, 49), ("Filet de badminton", 19, 79),
                        ("Maillot officiel", 29, 99), ("Raquette de tennis", 29, 199)),
    "SOIN_VISAGE": (("Nettoyant moussant", 7, 29), ("Creme hydratante", 9, 59),
                    ("Serum a l'acide hyaluronique", 15, 129)),
    "PARFUM": (("Eau de toilette", 25, 89), ("Eau de parfum", 39, 159),
               ("Coffret parfum", 45, 219)),
    "CAPILLAIRE": (("Shampooing reparateur", 5, 19), ("Seche-cheveux", 19, 129),
                   ("Lisseur ceramique", 25, 189)),
    "APPAREIL_SOIN": (("Tondeuse a barbe", 15, 79), ("Brosse a dents electrique", 19, 149),
                      ("Epilateur lumiere pulsee", 99, 449)),
    "ROMAN": (("Roman policier", 6, 19), ("Roman historique", 7, 22),
              ("Recueil de nouvelles", 6, 18)),
    "BD_MANGA": (("Manga tome unique", 7, 13), ("Bande dessinee", 10, 25),
                 ("Coffret integrale", 29, 89)),
    "SCOLAIRE": (("Cahier d'exercices", 5, 15), ("Manuel scolaire", 12, 35),
                 ("Annales du bac", 9, 25)),
    "FOURNITURE": (("Lot de stylos", 3, 15), ("Cahier grand format", 3, 12),
                   ("Trousse garnie", 9, 35), ("Agenda", 6, 29)),
}

BRANDS = (
    "Nexora", "Volturn", "Kaelis", "Orbisone", "Ferrata", "Miravel", "Zephyros",
    "Alderin", "Cassio", "Novaris", "Tessero", "Lumibel", "Aurentis", "Vantar",
    "Solvane", "Peritas", "Ombrelle", "Kavrio", "Steltis", "Yldra",
)

# Villes reelles par pays. Un referentiel fige plutot que des locales Faker :
# la generation reste identique quelle que soit la version de la bibliotheque
# installee, ce qui est indispensable pour un jeu de donnees reproductible.
CITIES: dict[str, tuple[str, ...]] = {
    "FR": ("Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux", "Nantes", "Lille", "Strasbourg"),
    "BE": ("Bruxelles", "Anvers", "Gand", "Liege", "Charleroi", "Bruges"),
    "CH": ("Geneve", "Zurich", "Lausanne", "Berne", "Bale", "Lugano"),
    "LU": ("Luxembourg", "Esch-sur-Alzette", "Differdange", "Dudelange"),
    "DE": ("Berlin", "Munich", "Hambourg", "Cologne", "Francfort", "Stuttgart"),
    "NL": ("Amsterdam", "Rotterdam", "La Haye", "Utrecht", "Eindhoven"),
    "ES": ("Madrid", "Barcelone", "Valence", "Seville", "Bilbao", "Malaga"),
    "IT": ("Rome", "Milan", "Naples", "Turin", "Bologne", "Florence"),
}

# Repartition des adresses : le marche principal est la France, les autres pays
# forment une longue traine. Une repartition uniforme donnerait une carte
# Kibana sans relief.
COUNTRY_WEIGHTS: dict[str, float] = {
    "FR": 0.58, "BE": 0.09, "CH": 0.06, "LU": 0.03,
    "DE": 0.08, "NL": 0.05, "ES": 0.06, "IT": 0.05,
}

# Longueur de la partie numerique du code postal, par pays.
POSTAL_DIGITS: dict[str, int] = {
    "FR": 5, "BE": 4, "CH": 4, "LU": 4, "DE": 5, "NL": 4, "ES": 5, "IT": 5,
}

STREET_TYPES = ("rue", "avenue", "boulevard", "impasse", "allee", "chemin", "place")

STREET_NAMES = (
    "des Lilas", "Victor Hugo", "de la Republique", "des Ecoles", "Gambetta",
    "de la Gare", "des Peupliers", "Jean Jaures", "du Marche", "des Rosiers",
    "Pasteur", "de Verdun", "des Acacias", "Emile Zola", "du Moulin",
    "de la Paix", "des Vignes", "Saint-Martin", "du Stade", "des Tilleuls",
)

# Statuts et leur poids. Le pipeline devant produire un chiffre d'affaires
# credible, les commandes livrees dominent largement.
ORDER_STATUS_WEIGHTS: dict[str, float] = {
    "DELIVERED": 0.63, "SHIPPED": 0.11, "PAID": 0.09,
    "PENDING": 0.06, "CANCELLED": 0.07, "RETURNED": 0.04,
}

# Statuts possibles pour une commande recente : une commande passee hier ne peut
# pas etre deja livree. Sans cette regle, les derniers jours du dashboard
# auraient un profil de statuts incoherent avec le reste de l'historique.
RECENT_STATUS_WEIGHTS: dict[str, float] = {
    "PENDING": 0.45, "PAID": 0.35, "SHIPPED": 0.20,
}

PAYMENT_METHOD_WEIGHTS: dict[int, float] = {1: 0.62, 2: 0.20, 3: 0.06, 4: 0.04, 5: 0.08}

LOYALTY_WEIGHTS: dict[str, float] = {"BRONZE": 0.50, "SILVER": 0.30, "GOLD": 0.15, "PLATINUM": 0.05}

# Saisonnalite mensuelle du commerce en ligne : pic de fin d'annee, creux de
# janvier et d'aout. C'est ce relief qui rendra la courbe Kibana parlante.
MONTH_WEIGHTS: dict[int, float] = {
    1: 0.80, 2: 0.85, 3: 0.95, 4: 1.00, 5: 1.05, 6: 0.95,
    7: 0.90, 8: 0.80, 9: 1.05, 10: 1.10, 11: 1.90, 12: 2.10,
}

# Profil horaire des commandes : creux nocturne, pic en soiree.
HOUR_WEIGHTS: tuple[float, ...] = (
    0.3, 0.2, 0.1, 0.1, 0.1, 0.2, 0.5, 1.0, 1.6, 1.8, 1.9, 1.7,
    1.5, 1.6, 1.8, 1.9, 2.0, 2.3, 2.8, 3.0, 2.7, 2.1, 1.3, 0.7,
)

# Nombre de lignes par commande : un panier moyen de 2 a 3 articles.
LINES_PER_ORDER_WEIGHTS: dict[int, float] = {1: 0.34, 2: 0.26, 3: 0.17, 4: 0.11, 5: 0.06, 6: 0.03, 7: 0.02, 8: 0.01}

# Remises accordees. Une commande sur cinq environ beneficie d'une remise.
DISCOUNT_WEIGHTS: dict[float, float] = {0: 0.80, 5: 0.07, 10: 0.06, 15: 0.04, 20: 0.03}

FREE_SHIPPING_THRESHOLD = 60.0
SHIPPING_FEE = 4.90

# Exposants pilotant le tirage des produits vendus.
#
# RANK : loi de puissance sur un rang aleatoire, qui produit la concentration
#   classique du commerce (une minorite de references fait la majorite des
#   ventes).
#
# PRICE : penalite appliquee au prix. Sans elle, un velo a 2 000 EUR serait
#   vendu aussi souvent qu'un stylo a 3 EUR, et le panier moyen atteindrait
#   plusieurs centaines d'euros -- un magasin qui ne vendrait que des
#   televiseurs. Dans un catalogue reel, les volumes se concentrent sur les
#   articles bon marche.
#
# Les deux valeurs sont calibrees ensemble : un panier median d'environ
# 60 EUR, et une concentration ou 20 % des references portent 80 % des
# lignes vendues. Augmenter la penalite de prix seule ferait tomber tout le
# volume sur les articles les moins chers, avec une concentration de 95 %
# sans rapport avec un catalogue reel.
PRODUCT_RANK_EXPONENT = 0.60
PRODUCT_PRICE_EXPONENT = 1.00
