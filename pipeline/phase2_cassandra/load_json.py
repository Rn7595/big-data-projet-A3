"""Chargement des documents JSON denormalises dans Cassandra.

Ce script ne se connecte jamais a Oracle : il ne connait qu'un chemin de
fichier. C'est ce qui permet a la phase 1 d'etre eteinte avant que celle-ci ne
demarre.

Un meme document alimente trois tables, chacune repondant a une requete
differente. C'est la mise en oeuvre concrete du principe "une table par
requete" : la duplication est faite ici, une fois, a l'ecriture.

Strategie d'ecriture : `execute_concurrent_with_args` maintient un nombre borne
de requetes en vol. On evite deliberement BatchStatement, qui est un
anti-pattern des qu'un lot touche plusieurs partitions : le coordinateur devrait
alors attendre tous les noeuds concernes, et le lot deviendrait plus lent que
les ecritures individuelles qu'il pretend remplacer. Un batch Cassandra sert a
garantir l'atomicite au sein d'une partition, pas a grouper pour la
performance.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterator

from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement

from pipeline.config import CASSANDRA, JSON_DIR, REPORT_DIR, ensure_dirs
from pipeline.phase2_cassandra import session as cass
from pipeline.phase2_cassandra.session import OrderItem
from pipeline.utils import get_logger, step

LOGGER = get_logger("phase2.chargement")

# Nombre de requetes maintenues en vol simultanement. Au-dela, un noeud unique
# commence a rejeter des ecritures pour surcharge (OverloadedErrorMessage).
CONCURRENCY = 64

# Les parametres sont accumules par tranches avant d'etre envoyes : cela borne
# la memoire du chargeur sans multiplier les allers-retours.
CHUNK_SIZE = 5_000

COLUMNS = {
    "orders_by_customer": [
        "customer_id", "order_date", "order_id", "order_ref", "order_status",
        "payment_method", "payment_label", "shipping_amount", "customer_email",
        "customer_first_name", "customer_last_name", "loyalty_tier", "shipping_city",
        "shipping_postal_code", "shipping_country_code", "shipping_country_name",
        "shipping_region", "items", "items_count", "total_quantity", "total_amount",
    ],
    "order_by_id": [
        "order_id", "order_ref", "order_date", "order_status", "payment_method",
        "payment_label", "shipping_amount", "customer_id", "customer_email",
        "customer_first_name", "customer_last_name", "loyalty_tier", "shipping_city",
        "shipping_postal_code", "shipping_country_code", "shipping_country_name",
        "shipping_region", "items", "items_count", "total_quantity", "total_amount",
    ],
    "sales_by_category_month": [
        "category_id", "year_month", "order_date", "order_id", "line_no",
        "category_name", "parent_category_id", "parent_category_name", "product_id",
        "sku", "product_name", "brand", "quantity", "unit_price", "discount_pct",
        "line_amount", "customer_id", "loyalty_tier", "order_status", "country_code",
        "country_name", "region", "city",
    ],
    "products_by_category": [
        "category_id", "product_name", "product_id", "sku", "brand", "unit_price",
        "is_active", "created_at", "category_code", "category_name",
        "parent_category_id", "parent_category_name",
    ],
}


def insert_statement(table: str) -> str:
    columns = COLUMNS[table]
    placeholders = ", ".join("?" * len(columns))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"


def to_decimal(value) -> Decimal | None:
    """Convertit un nombre JSON en Decimal.

    Les colonnes monetaires sont de type `decimal` et non `double` : un double
    ne peut pas representer exactement 19,90 et l'erreur s'accumulerait sur des
    centaines de milliers de lignes agregees. Oracle serialise en revanche
    20,00 sous la forme du JSON `20`, donc un entier : la conversion doit
    accepter les deux et passer par str pour ne pas reintroduire l'imprecision
    du binaire flottant.
    """
    if value is None:
        return None
    return Decimal(str(value))


def read_documents(path: Path) -> Iterator[dict]:
    """Lit un fichier JSON Lines document par document, sans le charger en entier."""
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def build_order_values(document: dict) -> dict:
    """Aplati un document de commande en un dictionnaire de colonnes."""
    customer = document["customer"]
    address = document["shipping_address"]
    items = [
        OrderItem(
            line_no=item["line_no"],
            product_id=item["product_id"],
            sku=item["sku"],
            product_name=item["product_name"],
            brand=item["brand"],
            category_id=item["category_id"],
            category_name=item["category_name"],
            parent_category_id=item["parent_category_id"],
            parent_category_name=item["parent_category_name"],
            quantity=item["quantity"],
            unit_price=to_decimal(item["unit_price"]),
            discount_pct=to_decimal(item["discount_pct"]),
            line_amount=to_decimal(item["line_amount"]),
        )
        for item in document["items"]
    ]
    return {
        "order_id": document["order_id"],
        "order_ref": document["order_ref"],
        "order_date": datetime.fromisoformat(document["order_date"]),
        "order_status": document["order_status"],
        "payment_method": document["payment_method"],
        "payment_label": document["payment_label"],
        "shipping_amount": to_decimal(document["shipping_amount"]),
        "customer_id": customer["customer_id"],
        "customer_email": customer["email"],
        "customer_first_name": customer["first_name"],
        "customer_last_name": customer["last_name"],
        "loyalty_tier": customer["loyalty_tier"],
        "shipping_city": address["city"],
        "shipping_postal_code": address["postal_code"],
        "shipping_country_code": address["country_code"],
        "shipping_country_name": address["country_name"],
        "shipping_region": address["region"],
        "items": items,
        "items_count": document["items_count"],
        "total_quantity": document["total_quantity"],
        "total_amount": to_decimal(document["total_amount"]),
    }


def build_sales_values(document: dict) -> Iterator[dict]:
    """Eclate une commande en une ligne par ligne de commande.

    La granularite descend de la commande a la ligne : c'est necessaire pour
    imputer chaque montant a sa categorie. Une commande touchant trois
    categories alimentera donc trois partitions differentes.
    """
    customer = document["customer"]
    address = document["shipping_address"]
    order_date = datetime.fromisoformat(document["order_date"])
    for item in document["items"]:
        yield {
            "category_id": item["category_id"],
            "year_month": document["order_year_month"],
            "order_date": order_date,
            "order_id": document["order_id"],
            "line_no": item["line_no"],
            "category_name": item["category_name"],
            "parent_category_id": item["parent_category_id"],
            "parent_category_name": item["parent_category_name"],
            "product_id": item["product_id"],
            "sku": item["sku"],
            "product_name": item["product_name"],
            "brand": item["brand"],
            "quantity": item["quantity"],
            "unit_price": to_decimal(item["unit_price"]),
            "discount_pct": to_decimal(item["discount_pct"]),
            "line_amount": to_decimal(item["line_amount"]),
            "customer_id": customer["customer_id"],
            "loyalty_tier": customer["loyalty_tier"],
            "order_status": document["order_status"],
            "country_code": address["country_code"],
            "country_name": address["country_name"],
            "region": address["region"],
            "city": address["city"],
        }


def build_product_values(document: dict) -> dict:
    return {
        "category_id": document["category_id"],
        "product_name": document["product_name"],
        "product_id": document["product_id"],
        "sku": document["sku"],
        "brand": document["brand"],
        "unit_price": to_decimal(document["unit_price"]),
        "is_active": bool(document["is_active"]),
        "created_at": date.fromisoformat(document["created_at"]),
        "category_code": document["category_code"],
        "category_name": document["category_name"],
        "parent_category_id": document["parent_category_id"],
        "parent_category_name": document["parent_category_name"],
    }


class TableWriter:
    """Accumule des lignes puis les ecrit par tranches concurrentes."""

    def __init__(self, session, table: str) -> None:
        self.table = table
        self.columns = COLUMNS[table]
        self.session = session
        self.prepared = session.prepare(insert_statement(table))
        self.buffer: list[tuple] = []
        self.written = 0

    def add(self, values: dict) -> None:
        # La projection du dictionnaire sur la liste de colonnes garantit que
        # l'ordre des parametres suit toujours celui de l'instruction preparee.
        self.buffer.append(tuple(values[column] for column in self.columns))
        if len(self.buffer) >= CHUNK_SIZE:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        execute_concurrent_with_args(
            self.session, self.prepared, self.buffer,
            concurrency=CONCURRENCY, raise_on_first_error=True,
        )
        self.written += len(self.buffer)
        self.buffer.clear()


def load_orders(session) -> dict[str, int]:
    writers = {
        table: TableWriter(session, table)
        for table in ("orders_by_customer", "order_by_id", "sales_by_category_month")
    }
    documents = 0
    for document in read_documents(JSON_DIR / "orders.jsonl"):
        values = build_order_values(document)
        writers["orders_by_customer"].add(values)
        writers["order_by_id"].add(values)
        for line in build_sales_values(document):
            writers["sales_by_category_month"].add(line)
        documents += 1
        if documents % 10_000 == 0:
            LOGGER.info("    %d commandes traitees", documents)

    for writer in writers.values():
        writer.flush()
    LOGGER.info("    %d commandes lues", documents)
    return {table: writer.written for table, writer in writers.items()}


def load_products(session) -> dict[str, int]:
    writer = TableWriter(session, "products_by_category")
    for document in read_documents(JSON_DIR / "products.jsonl"):
        writer.add(build_product_values(document))
    writer.flush()
    return {"products_by_category": writer.written}


def count_rows(session, table: str) -> int:
    """Compte les lignes d'une table.

    A n'utiliser que pour un controle ponctuel : un COUNT(*) sans cle de
    partition force le balayage de toutes les partitions du cluster. C'est
    precisement ce que le modele cherche a eviter en usage normal, d'ou le
    delai d'attente allonge.
    """
    statement = SimpleStatement(f"SELECT COUNT(*) AS total FROM {table}", fetch_size=None)
    return session.execute(statement, timeout=300).one().total


def main() -> None:
    ensure_dirs()
    orders_file = JSON_DIR / "orders.jsonl"
    if not orders_file.exists():
        raise RuntimeError(f"{orders_file} est absent : la phase 1 doit etre executee avant.")

    cluster, session = cass.connect()
    try:
        cass.register_types(cluster, CASSANDRA.keyspace)
        session.set_keyspace(CASSANDRA.keyspace)

        with step(LOGGER, "chargement des commandes"):
            written = load_orders(session)
        with step(LOGGER, "chargement du catalogue produit"):
            written.update(load_products(session))

        for table, count in written.items():
            LOGGER.info("  %-28s %8d ligne(s) ecrites", table, count)

        with step(LOGGER, "verification des volumetries"):
            counted = {table: count_rows(session, table) for table in written}

        report = {"ecrites": written, "comptees": counted}
        ecarts = [t for t in written if written[t] != counted[t]]
        for table in written:
            LOGGER.info("  %-28s ecrites=%-8d comptees=%-8d %s",
                        table, written[table], counted[table],
                        "ECART" if table in ecarts else "ok")
        if ecarts:
            raise RuntimeError(f"Ecart entre lignes ecrites et lignes lues : {ecarts}")
    finally:
        cluster.shutdown()

    report_file = REPORT_DIR / "phase2_chargement.json"
    report_file.write_text(json.dumps(report, indent=2), encoding="utf-8")
    LOGGER.info("Rapport ecrit dans %s", report_file)


if __name__ == "__main__":
    main()
