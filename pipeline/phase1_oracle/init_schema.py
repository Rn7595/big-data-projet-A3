"""Creation du schema Oracle : suppression, tables, index, donnees de reference.

Le script est rejouable a volonte : il repart systematiquement d'un schema
vide. C'est ce qui permet de rejouer la phase 1 pendant la demonstration sans
se retrouver avec des donnees a moitie chargees.
"""

from __future__ import annotations

from pipeline.config import SQL_DIR
from pipeline.phase1_oracle import db
from pipeline.utils import get_logger, step

LOGGER = get_logger("phase1.schema")

SCRIPTS = [
    ("suppression du schema existant", "00_drop.sql"),
    ("creation des tables", "01_schema.sql"),
    ("creation des index", "02_indexes.sql"),
    ("chargement des donnees de reference", "03_seed_reference.sql"),
]


def main() -> None:
    with db.connect() as connection:
        LOGGER.info("Connecte a %s en tant que %s", connection.dsn, connection.username)
        for label, filename in SCRIPTS:
            with step(LOGGER, label):
                executed = db.run_script(connection, SQL_DIR / filename)
                LOGGER.info("    %s : %d instruction(s)", filename, executed)

        counts = db.table_counts(connection)
    LOGGER.info("Schema pret : %s", ", ".join(f"{t}={n}" for t, n in counts.items()))


if __name__ == "__main__":
    main()
