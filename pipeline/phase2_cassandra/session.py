"""Connexion a Cassandra et execution des fichiers CQL.

Le type utilisateur order_item est declare ici sous forme de namedtuple et
enregistre aupres du pilote : celui-ci sait alors serialiser un objet Python en
UDT Cassandra, et inversement. Sans cet enregistrement, l'insertion d'une
collection list<frozen<order_item>> echoue.
"""

from __future__ import annotations

import time
from collections import namedtuple
from pathlib import Path

from cassandra.cluster import Cluster, Session

from pipeline.config import CASSANDRA
from pipeline.utils import get_logger

LOGGER = get_logger("phase2.session")

# L'ordre des champs importe peu, le pilote associe par nom ; en revanche les
# noms doivent correspondre exactement a ceux du type declare dans le CQL.
OrderItem = namedtuple("OrderItem", [
    "line_no",
    "product_id",
    "sku",
    "product_name",
    "brand",
    "category_id",
    "category_name",
    "parent_category_id",
    "parent_category_name",
    "quantity",
    "unit_price",
    "discount_pct",
    "line_amount",
])


def connect(keyspace: str | None = None, attempts: int = 12, delay: int = 5) -> tuple[Cluster, Session]:
    """Ouvre une session, en reessayant tant que le noeud n'accepte pas les clients.

    Le healthcheck du conteneur passe des que `nodetool status` repond, ce qui
    precede de quelques secondes l'ouverture du port CQL. Ces tentatives
    absorbent cet ecart.
    """
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            cluster = Cluster([CASSANDRA.host], port=CASSANDRA.port)
            session = cluster.connect(keyspace) if keyspace else cluster.connect()
            LOGGER.info("Connecte a Cassandra sur %s:%s", CASSANDRA.host, CASSANDRA.port)
            return cluster, session
        except Exception as error:  # noqa: BLE001 - on retente quelle que soit la cause
            last_error = error
            LOGGER.info("Cassandra pas encore joignable (tentative %d/%d)", attempt, attempts)
            time.sleep(delay)
    raise RuntimeError(f"Connexion a Cassandra impossible : {last_error}")


def register_types(cluster: Cluster, keyspace: str) -> None:
    """Associe le type utilisateur CQL a sa representation Python."""
    cluster.register_user_type(keyspace, "order_item", OrderItem)


def split_statements(cql_text: str) -> list[str]:
    """Decoupe un fichier CQL sur les points-virgules, commentaires retires."""
    lines = [line for line in cql_text.splitlines() if not line.strip().startswith("--")]
    statements = []
    for raw in "\n".join(lines).split(";"):
        statement = raw.strip()
        if statement:
            statements.append(statement)
    return statements


def run_cql_file(session: Session, path: Path) -> int:
    """Execute toutes les instructions d'un fichier CQL."""
    statements = split_statements(path.read_text(encoding="utf-8"))
    for statement in statements:
        session.execute(statement)
    return len(statements)
