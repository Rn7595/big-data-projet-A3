"""Referentiels utilises par le generateur de donnees.

Ces tables de correspondance sont isolees du code de generation pour garder
`generate_data.py` lisible : on y trouve les gammes de prix par categorie, les
marques, et la geographie utilisee pour les adresses.
"""

from __future__ import annotations

# Pour chaque code de categorie feuille : les intitules de produits possibles et
# la fourchette de prix. Faire varier les prix par categorie evite un chiffre
# d'affaires uniforme et rend les agregations Kibana lisibles : un televiseur ne
# doit pas peser autant qu'un cahier.
CATALOGUE: dict[str, tuple[tuple[str, ...], float, float]] = {
    "PC_PORTABLE": (("Ordinateur portable", "Ultrabook", "PC portable gamer"), 449, 2299),
    "PC_BUREAU": (("Ordinateur de bureau", "Tour gamer", "Mini PC"), 379, 1899),
    "PERIPHERIQUE": (("Clavier mecanique", "Souris sans fil", "Ecran 27 pouces", "Webcam"), 19, 549),
    "STOCKAGE": (("Disque SSD", "Disque dur externe", "Cle USB", "Carte memoire"), 12, 329),
    "TELEVISEUR": (("Televiseur OLED", "Televiseur QLED", "Televiseur LED"), 299, 2499),
    "CASQUE": (("Casque bluetooth", "Casque a reduction de bruit", "Ecouteurs sans fil"), 29, 449),
    "ENCEINTE": (("Enceinte nomade", "Enceinte connectee", "Barre de son"), 39, 699),
    "HOME_CINEMA": (("Videoprojecteur", "Ampli home cinema", "Kit 5.1"), 199, 1799),
    "SMARTPHONE": (("Smartphone", "Smartphone pliable", "Telephone reconditionne"), 149, 1449),
    "TABLETTE": (("Tablette tactile", "Tablette graphique", "Liseuse"), 89, 1249),
    "ACC_MOBILE": (("Coque de protection", "Chargeur rapide", "Batterie externe", "Verre trempe"), 8, 89),
    "OBJET_CONNECTE": (("Montre connectee", "Bracelet d'activite", "Balance connectee"), 39, 599),
    "PETIT_ELECTRO": (("Robot patissier", "Cafetiere expresso", "Aspirateur balai", "Friteuse sans huile"), 49, 899),
    "GROS_ELECTRO": (("Lave-linge", "Refrigerateur combine", "Lave-vaisselle", "Four encastrable"), 299, 1699),
    "ARTS_TABLE": (("Service d'assiettes", "Set de couteaux", "Cocotte en fonte", "Verres a pied"), 19, 349),
    "LINGE_MAISON": (("Parure de lit", "Couette 4 saisons", "Lot de serviettes", "Rideaux occultants"), 15, 249),
    "VET_HOMME": (("Chemise en lin", "Pull en laine", "Jean droit", "Veste matelassee"), 19, 249),
    "VET_FEMME": (("Robe portefeuille", "Blouse en soie", "Manteau long", "Jean taille haute"), 22, 289),
    "CHAUSSURES": (("Baskets en cuir", "Bottines", "Sandales", "Chaussures de ville"), 29, 279),
    "MAROQUINERIE": (("Sac a main", "Portefeuille", "Sac a dos urbain", "Ceinture en cuir"), 25, 429),
    "FITNESS": (("Tapis de yoga", "Halteres reglables", "Rameur pliable", "Banc de musculation"), 15, 899),
    "CYCLISME": (("Velo de route", "VTT tout suspendu", "Casque de velo", "Compteur GPS"), 29, 2399),
    "RANDONNEE": (("Sac de randonnee", "Tente 2 places", "Chaussures de trek", "Batons telescopiques"), 29, 499),
    "SPORT_COLLECTIF": (("Ballon de football", "Maillot officiel", "Raquette de tennis", "Filet de badminton"), 12, 199),
    "SOIN_VISAGE": (("Creme hydratante", "Serum a l'acide hyaluronique", "Nettoyant moussant"), 9, 129),
    "PARFUM": (("Eau de parfum", "Eau de toilette", "Coffret parfum"), 29, 219),
    "CAPILLAIRE": (("Shampooing reparateur", "Seche-cheveux", "Lisseur ceramique"), 7, 189),
    "APPAREIL_SOIN": (("Brosse a dents electrique", "Epilateur lumiere pulsee", "Tondeuse a barbe"), 19, 449),
    "ROMAN": (("Roman policier", "Roman historique", "Recueil de nouvelles"), 7, 29),
    "BD_MANGA": (("Bande dessinee", "Manga tome unique", "Coffret integrale"), 9, 89),
    "SCOLAIRE": (("Manuel scolaire", "Annales du bac", "Cahier d'exercices"), 6, 39),
    "FOURNITURE": (("Lot de stylos", "Cahier grand format", "Trousse garnie", "Agenda"), 3, 45),
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
PRODUCT_PRICE_EXPONENT = 1.15
