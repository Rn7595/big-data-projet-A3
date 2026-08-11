"""Creation du keyspace, du type utilisateur et des quatre tables Cassandra.

Le script est rejouable : les tables existantes sont supprimees avant d'etre
recreees, de facon a repartir d'un modele propre. Le keyspace, lui, est cree
avec IF NOT EXISTS et conserve.
"""

from __future__ import annotations

from pipeline.config import CASSANDRA, CQL_DIR
from pipeline.phase2_cassandra import session as cass
from pipeline.utils import get_logger, step

LOGGER = get_logger("phase2.schema")

TABLES = ["orders_by_customer", "order_by_id", "sales_by_category_month", "products_by_category"]


def main() -> None:
    cluster, session = cass.connect()
    try:
        with step(LOGGER, "creation du keyspace"):
            count = cass.run_cql_file(session, CQL_DIR / "01_keyspace.cql")
            LOGGER.info("    %d instruction(s)", count)

        session.set_keyspace(CASSANDRA.keyspace)

        with step(LOGGER, "suppression des tables existantes"):
            for table in TABLES:
                session.execute(f"DROP TABLE IF EXISTS {table}")
            session.execute("DROP TYPE IF EXISTS order_item")

        with step(LOGGER, "creation du type et des tables"):
            count = cass.run_cql_file(session, CQL_DIR / "02_tables.cql")
            LOGGER.info("    %d instruction(s)", count)

        rows = session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s",
            (CASSANDRA.keyspace,),
        )
        LOGGER.info("Tables du keyspace %s : %s",
                    CASSANDRA.keyspace, ", ".join(sorted(row.table_name for row in rows)))
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
