"""Generation du jeu de donnees e-commerce dans Oracle.

Pourquoi generer plutot que telecharger un jeu de donnees existant :

  * les jeux publics sont presque toujours livres a plat, deja denormalises.
    Les charger reviendrait a sauter l'etape que le sujet demande justement de
    realiser, la denormalisation depuis un schema relationnel ;
  * la generation garantit que toutes les contraintes du schema sont
    satisfaites, y compris les regles qu'aucune contrainte declarative ne peut
    exprimer (une commande posterieure a l'inscription du client, une adresse
    de livraison appartenant bien a ce client) ;
  * la graine aleatoire rend le jeu reproductible : le pipeline peut etre
    rejoue de bout en bout et les comptages restent comparables d'une phase a
    l'autre ;
  * la volumetrie est un parametre, ce qui permet de repeter la demonstration
    sur un echantillon reduit puis de charger le jeu complet.

Le realisme n'est pas cosmetique : sans saisonnalite, sans loi de Pareto sur
les produits et sans profil horaire, les graphiques Kibana de la phase 4
seraient des lignes plates dont il n'y aurait rien a dire.

Des anomalies sont injectees volontairement sur environ 1 % des lignes (casse
et espaces parasites) pour donner un objet reel a l'etape de nettoyage.
"""

from __future__ import annotations

import calendar
import random
import unicodedata
from datetime import date, datetime, timedelta
from itertools import accumulate
from typing import Sequence

from faker import Faker

from pipeline.config import DATASET
from pipeline.phase1_oracle import db
from pipeline.phase1_oracle import reference_data as ref
from pipeline.utils import get_logger, step

LOGGER = get_logger("phase1.generate")

BATCH_SIZE = 20_000

# Taux d'injection des anomalies, exprimes en proportion des lignes concernees.
DIRTY_EMAIL_RATE = 0.012
DIRTY_NAME_RATE = 0.008
DIRTY_PRODUCT_RATE = 0.020
DIRTY_CITY_RATE = 0.010
DIRTY_POSTAL_RATE = 0.012
EMPTY_ORDER_RATE = 0.005

SQL_INSERT = {
    "products": """
        INSERT INTO products
          (product_id, sku, product_name, brand, category_id, unit_price, is_active, created_at)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
    """,
    "customers": """
        INSERT INTO customers
          (customer_id, email, first_name, last_name, birth_date, signup_date, loyalty_tier)
        VALUES (:1, :2, :3, :4, :5, :6, :7)
    """,
    "addresses": """
        INSERT INTO addresses
          (address_id, customer_id, address_type, street, city, postal_code, country_code)
        VALUES (:1, :2, :3, :4, :5, :6, :7)
    """,
    "orders": """
        INSERT INTO orders
          (order_id, order_ref, customer_id, order_date, order_status,
           payment_method_id, shipping_address_id, billing_address_id, shipping_amount)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
    """,
    "order_items": """
        INSERT INTO order_items
          (order_id, line_no, product_id, quantity, unit_price, discount_pct)
        VALUES (:1, :2, :3, :4, :5, :6)
    """,
}


class WeightedPicker:
    """Tirage pondere avec poids cumules precalcules.

    `random.choices` recalcule la somme des poids a chaque appel lorsqu'on lui
    passe `weights`. Sur plusieurs centaines de milliers de tirages, cela
    domine le temps de generation. En fournissant `cum_weights`, chaque tirage
    se reduit a une recherche dichotomique.
    """

    def __init__(self, rng: random.Random, weights: dict) -> None:
        self._rng = rng
        self._population = list(weights.keys())
        self._cumulative = list(accumulate(weights.values()))

    def pick(self):
        return self._rng.choices(self._population, cum_weights=self._cumulative, k=1)[0]

    def pick_many(self, count: int) -> list:
        return self._rng.choices(self._population, cum_weights=self._cumulative, k=count)


def strip_accents(text: str) -> str:
    """Retire les diacritiques : utilise pour fabriquer des adresses email valides."""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(char for char in decomposed if not unicodedata.combining(char))


class DataGenerator:
    def __init__(self, config=DATASET) -> None:
        self.config = config
        self.rng = random.Random(config.seed)
        self.faker = Faker("fr_FR")
        Faker.seed(config.seed)

        # Fenetre temporelle des commandes : les `history_months` derniers mois
        # complets, jusqu'a aujourd'hui.
        self.end_date = date.today()
        self.months = self._build_months(self.end_date, config.history_months)
        self.start_date = date(self.months[0][0], self.months[0][1], 1)
        # Les inscriptions demarrent un an avant les commandes : on veut des
        # clients deja anciens au debut de l'historique, sinon tous les clients
        # semblent nouveaux et la segmentation de la phase 3 perd son sens.
        self.signup_start = self.start_date - timedelta(days=365)

        self.country_picker = WeightedPicker(self.rng, ref.COUNTRY_WEIGHTS)
        self.status_picker = WeightedPicker(self.rng, ref.ORDER_STATUS_WEIGHTS)
        self.recent_status_picker = WeightedPicker(self.rng, ref.RECENT_STATUS_WEIGHTS)
        self.payment_picker = WeightedPicker(self.rng, ref.PAYMENT_METHOD_WEIGHTS)
        self.loyalty_picker = WeightedPicker(self.rng, ref.LOYALTY_WEIGHTS)
        self.lines_picker = WeightedPicker(self.rng, ref.LINES_PER_ORDER_WEIGHTS)
        self.discount_picker = WeightedPicker(self.rng, ref.DISCOUNT_WEIGHTS)
        self.hour_picker = WeightedPicker(self.rng, dict(enumerate(ref.HOUR_WEIGHTS)))

        self._month_cumulative: dict[int, list[float]] = {}
        self.anomalies = {key: 0 for key in
                          ("email", "nom", "produit", "ville", "code_postal", "commande_vide")}

    # ------------------------------------------------------------------ dates

    @staticmethod
    def _build_months(end: date, history_months: int) -> list[tuple[int, int]]:
        months: list[tuple[int, int]] = []
        year, month = end.year, end.month
        for _ in range(history_months):
            months.append((year, month))
            month -= 1
            if month == 0:
                year, month = year - 1, 12
        return list(reversed(months))

    def _month_index(self, day: date) -> int:
        """Indice du mois d'une date dans la fenetre, borne au debut de celle-ci."""
        if day <= self.start_date:
            return 0
        return (day.year - self.months[0][0]) * 12 + (day.month - self.months[0][1])

    def _cumulative_from(self, first_index: int) -> list[float]:
        """Poids cumules de saisonnalite a partir d'un mois donne (memoise)."""
        if first_index not in self._month_cumulative:
            weights = [ref.MONTH_WEIGHTS[month] for _, month in self.months[first_index:]]
            self._month_cumulative[first_index] = list(accumulate(weights))
        return self._month_cumulative[first_index]

    def order_datetime(self, signup_date: date) -> datetime:
        """Tire une date de commande posterieure a l'inscription et dans la fenetre.

        Le mois est tire selon la saisonnalite, puis le jour uniformement dans
        la plage valide de ce mois. Tirer le mois d'abord evite d'avoir a
        rejeter des dates invalides, et borner le jour a la date d'inscription
        evite l'artefact d'un pic de commandes le jour meme de l'inscription.
        """
        first_index = self._month_index(signup_date)
        candidates = range(first_index, len(self.months))
        index = self.rng.choices(candidates, cum_weights=self._cumulative_from(first_index), k=1)[0]
        year, month = self.months[index]

        first_day = 1
        if signup_date > self.start_date and (year, month) == (signup_date.year, signup_date.month):
            first_day = signup_date.day
        last_day = calendar.monthrange(year, month)[1]
        if (year, month) == (self.end_date.year, self.end_date.month):
            last_day = min(last_day, self.end_date.day)

        day = self.rng.randint(first_day, max(first_day, last_day))
        return datetime(year, month, day, self.hour_picker.pick(),
                        self.rng.randrange(60), self.rng.randrange(60))

    # -------------------------------------------------------------- anomalies

    def _maybe_dirty_email(self, email: str) -> str:
        """Casse et espaces parasites : LOWER(TRIM(...)) restaure l'original, donc
        l'unicite de l'adresse est preservee et la contrainte tient."""
        if self.rng.random() < DIRTY_EMAIL_RATE:
            self.anomalies["email"] += 1
            return f"  {email.upper()} "
        return email

    def _maybe_dirty_name(self, name: str) -> str:
        if self.rng.random() < DIRTY_NAME_RATE:
            self.anomalies["nom"] += 1
            return f" {name} "
        return name

    def _maybe_dirty_label(self, label: str, rate: float, key: str) -> str:
        """Insere un double espace entre deux mots, comme une saisie manuelle."""
        if self.rng.random() < rate and " " in label:
            self.anomalies[key] += 1
            head, _, tail = label.partition(" ")
            return f"{head}  {tail}"
        return label

    def _maybe_dirty_city(self, city: str) -> str:
        """La plupart des villes tiennent en un mot : on salit par des espaces de
        bordure, que la regle de nettoyage traite au meme titre que les doubles
        espaces des villes composees."""
        if self.rng.random() < DIRTY_CITY_RATE:
            self.anomalies["ville"] += 1
            if " " in city:
                head, _, tail = city.partition(" ")
                return f"{head}  {tail} "
            return f"{city}  "
        return city

    def _maybe_dirty_postal(self, postal_code: str) -> str:
        if self.rng.random() < DIRTY_POSTAL_RATE and len(postal_code) > 2:
            self.anomalies["code_postal"] += 1
            middle = len(postal_code) // 2
            return f"{postal_code[:middle]} {postal_code[middle:]}"
        return postal_code

    # ------------------------------------------------------------- generation

    def products(self, leaf_categories: Sequence[tuple[int, str]]) -> list[tuple]:
        rows = []
        for product_id in range(1, self.config.nb_products + 1):
            category_id, category_code = self.rng.choice(leaf_categories)
            nouns, price_min, price_max = ref.CATALOGUE[category_code]
            brand = self.rng.choice(ref.BRANDS)
            noun = self.rng.choice(nouns)
            model = f"{self.rng.choice('ABCEGKMNPRSTVXZ')}{self.rng.randrange(10, 990)}"
            name = self._maybe_dirty_label(f"{noun} {brand} {model}", DIRTY_PRODUCT_RATE, "produit")

            # Les prix sont tires en echelle logarithmique : les articles
            # d'entree de gamme restent majoritaires, comme dans un vrai
            # catalogue, au lieu d'etre uniformement repartis jusqu'au haut de
            # gamme.
            ratio = self.rng.random() ** 1.6
            price = round(price_min + (price_max - price_min) * ratio, 2)
            price = max(price, 1.0)

            sku = f"{category_code[:4]}-{product_id:05d}"
            is_active = 0 if self.rng.random() < 0.06 else 1
            created_at = self.start_date - timedelta(days=self.rng.randrange(30, 400))
            rows.append((product_id, sku, name, brand, category_id, price, is_active, created_at))
        return rows

    def customers(self) -> list[tuple]:
        rows = []
        used_emails: set[str] = set()
        window_days = (self.end_date - self.signup_start).days
        for customer_id in range(1, self.config.nb_customers + 1):
            first_name = self.faker.first_name()
            last_name = self.faker.last_name()

            local = f"{strip_accents(first_name)}.{strip_accents(last_name)}".lower()
            local = "".join(char for char in local if char.isalnum() or char == ".")
            email = f"{local}@{self.faker.free_email_domain()}"
            suffix = 1
            while email in used_emails:
                suffix += 1
                email = f"{local}{suffix}@{self.faker.free_email_domain()}"
            used_emails.add(email)

            signup_date = self.signup_start + timedelta(days=self.rng.randrange(window_days))
            birth_date = date(self.rng.randrange(1955, 2006), self.rng.randrange(1, 13),
                              self.rng.randrange(1, 29))
            rows.append((
                customer_id,
                self._maybe_dirty_email(email),
                self._maybe_dirty_name(first_name),
                self._maybe_dirty_name(last_name),
                birth_date,
                signup_date,
                self.loyalty_picker.pick(),
            ))
        return rows

    def addresses(self, customer_ids: Sequence[int]) -> tuple[list[tuple], dict[int, tuple[int, list[int]]]]:
        """Genere les adresses et l'index {client: (facturation, [livraisons])}.

        Cet index est indispensable pour que chaque commande reference des
        adresses appartenant a son propre client : une cle etrangere garantit
        que l'adresse existe, pas qu'elle est la bonne.
        """
        rows: list[tuple] = []
        index: dict[int, tuple[int, list[int]]] = {}
        address_id = 0
        for customer_id in customer_ids:
            country = self.country_picker.pick()
            city = self.rng.choice(ref.CITIES[country])
            postal_digits = ref.POSTAL_DIGITS[country]
            postal_code = "".join(str(self.rng.randrange(10)) for _ in range(postal_digits))
            if country == "NL":
                postal_code += "".join(self.rng.choice("ABCDEFGHJKLMNPRSTVWXZ") for _ in range(2))

            billing_id = None
            shipping_ids: list[int] = []
            nb_shipping = 2 if self.rng.random() < 0.25 else 1
            for position in range(1 + nb_shipping):
                address_id += 1
                address_type = "BILLING" if position == 0 else "SHIPPING"
                street = (f"{self.rng.randrange(1, 180)} {self.rng.choice(ref.STREET_TYPES)} "
                          f"{self.rng.choice(ref.STREET_NAMES)}")
                rows.append((
                    address_id,
                    customer_id,
                    address_type,
                    street,
                    self._maybe_dirty_city(city),
                    self._maybe_dirty_postal(postal_code),
                    country,
                ))
                if address_type == "BILLING":
                    billing_id = address_id
                else:
                    shipping_ids.append(address_id)
            index[customer_id] = (billing_id, shipping_ids)
        return rows, index

    def orders(
        self,
        signup_dates: dict[int, date],
        address_index: dict[int, tuple[int, list[int]]],
        products: Sequence[tuple],
    ) -> tuple[list[tuple], list[tuple]]:
        """Genere les commandes et leurs lignes.

        Les clients sont tires selon une loi de Pareto : une minorite de clients
        concentre la majorite des commandes. C'est ce desequilibre qui donne du
        sens a la segmentation RFM de la phase 3 et a la table Cassandra
        orders_by_customer, dont les partitions sont de tailles tres inegales.
        """
        customer_ids = list(signup_dates.keys())
        customer_picker = self._pareto_picker(customer_ids, exponent=1.15)

        product_ids = [row[0] for row in products]
        catalog_price = {row[0]: row[5] for row in products}
        product_picker = self._pareto_picker(product_ids, exponent=1.10)

        recent_threshold = datetime.combine(self.end_date - timedelta(days=12), datetime.min.time())

        order_rows: list[tuple] = []
        item_rows: list[tuple] = []

        # Tirages effectues en une fois : un appel par commande couterait plus
        # cher que la generation elle-meme.
        chosen_customers = customer_picker(self.config.nb_orders)
        line_counts = self.lines_picker.pick_many(self.config.nb_orders)
        payment_methods = self.payment_picker.pick_many(self.config.nb_orders)

        for offset in range(self.config.nb_orders):
            order_id = offset + 1
            customer_id = chosen_customers[offset]
            order_date = self.order_datetime(signup_dates[customer_id])
            billing_id, shipping_ids = address_index[customer_id]

            nb_lines = 0 if self.rng.random() < EMPTY_ORDER_RATE else line_counts[offset]
            if nb_lines == 0:
                self.anomalies["commande_vide"] += 1

            items_amount = 0.0
            seen_products: set[int] = set()
            line_no = 0
            for product_id in product_picker(nb_lines * 2):
                if line_no == nb_lines:
                    break
                if product_id in seen_products:
                    continue
                seen_products.add(product_id)
                line_no += 1

                quantity = 1 if self.rng.random() < 0.72 else self.rng.randint(2, 4)
                # Le prix facture s'ecarte legerement du prix catalogue :
                # c'est l'interet d'historiser unit_price dans ORDER_ITEMS.
                unit_price = round(catalog_price[product_id] * self.rng.uniform(0.92, 1.05), 2)
                unit_price = max(unit_price, 0.5)
                discount = self.discount_picker.pick()
                if order_date.month == 11:
                    discount = max(discount, self.discount_picker.pick())
                item_rows.append((order_id, line_no, product_id, quantity, unit_price, discount))
                items_amount += round(quantity * unit_price * (1 - discount / 100), 2)

            shipping = 0.0 if items_amount >= ref.FREE_SHIPPING_THRESHOLD else ref.SHIPPING_FEE
            if line_no == 0:
                shipping = 0.0
            status = (self.recent_status_picker.pick() if order_date >= recent_threshold
                      else self.status_picker.pick())

            order_rows.append((
                order_id,
                f"CMD-{order_id:08d}",
                customer_id,
                order_date,
                status,
                payment_methods[offset],
                self.rng.choice(shipping_ids),
                billing_id,
                shipping,
            ))
        return order_rows, item_rows

    def _pareto_picker(self, values: Sequence, exponent: float):
        """Construit un tireur pondere par une loi de puissance sur un rang aleatoire.

        Les valeurs sont d'abord melangees : sans cela, la popularite serait
        correlee a l'identifiant, et les produits populaires seraient tous en
        tete de table, ce qui fausserait toute analyse.
        """
        shuffled = list(values)
        self.rng.shuffle(shuffled)
        weights = [1.0 / (rank ** exponent) for rank in range(1, len(shuffled) + 1)]
        cumulative = list(accumulate(weights))

        def pick(count: int) -> list:
            if count <= 0:
                return []
            return self.rng.choices(shuffled, cum_weights=cumulative, k=count)

        return pick


def insert_many(connection, key: str, rows: Sequence[tuple]) -> None:
    """Insertion par lots.

    executemany transmet tout un lot en un seul aller-retour reseau et n'analyse
    la requete qu'une fois. Charger 200 000 lignes une par une prendrait
    plusieurs dizaines de minutes ; par lots de 20 000, quelques secondes.
    """
    statement = SQL_INSERT[key]
    with connection.cursor() as cursor:
        for start in range(0, len(rows), BATCH_SIZE):
            cursor.executemany(statement, rows[start:start + BATCH_SIZE])
            connection.commit()


def main() -> None:
    generator = DataGenerator()
    LOGGER.info(
        "Fenetre des commandes : %s -> %s (%d mois), graine %d",
        generator.start_date, generator.end_date, DATASET.history_months, DATASET.seed,
    )

    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT category_id, category_code FROM categories "
                "WHERE parent_category_id IS NOT NULL ORDER BY category_id"
            )
            leaf_categories = cursor.fetchall()
        if not leaf_categories:
            raise RuntimeError("Aucune categorie feuille : lancez d'abord init_schema.py")
        LOGGER.info("%d categories feuilles trouvees", len(leaf_categories))

        with step(LOGGER, f"generation et chargement de {DATASET.nb_products} produits"):
            products = generator.products(leaf_categories)
            insert_many(connection, "products", products)

        with step(LOGGER, f"generation et chargement de {DATASET.nb_customers} clients"):
            customers = generator.customers()
            insert_many(connection, "customers", customers)
            signup_dates = {row[0]: row[5] for row in customers}

        with step(LOGGER, "generation et chargement des adresses"):
            addresses, address_index = generator.addresses(list(signup_dates.keys()))
            insert_many(connection, "addresses", addresses)
            LOGGER.info("    %d adresses", len(addresses))

        with step(LOGGER, f"generation de {DATASET.nb_orders} commandes"):
            orders, items = generator.orders(signup_dates, address_index, products)

        with step(LOGGER, "chargement des commandes"):
            insert_many(connection, "orders", orders)

        with step(LOGGER, f"chargement de {len(items)} lignes de commande"):
            insert_many(connection, "order_items", items)

        counts = db.table_counts(connection)

    LOGGER.info("Volumetrie Oracle : %s", ", ".join(f"{t}={n}" for t, n in counts.items()))
    LOGGER.info(
        "Anomalies injectees pour l'etape de nettoyage : %s",
        ", ".join(f"{k}={v}" for k, v in generator.anomalies.items()),
    )


if __name__ == "__main__":
    main()
