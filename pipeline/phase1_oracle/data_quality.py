"""Nettoyage puis controles de qualite, avant l'extraction JSON.

L'ordre importe : on nettoie d'abord, on controle ensuite. Les controles
verifient donc le resultat du nettoyage et non les donnees brutes, et un
controle bloquant qui echoue signale une anomalie que le nettoyage ne sait pas
traiter, c'est-a-dire un vrai defaut.

Placer cette etape avant l'extraction est un choix delibere : une donnee sale
qui franchit la frontiere JSON se retrouve recopiee dans Cassandra, dans
Parquet puis dans Elasticsearch, ou plus rien ne permet de la rattacher a son
origine. On corrige a la source, une fois.
"""

from __future__ import annotations

from pipeline.config import SQL_DIR
from pipeline.phase1_oracle import db
from pipeline.utils import get_logger, step

LOGGER = get_logger("phase1.qualite")

MAX_SAMPLE_ROWS = 5


class QualityCheckFailed(RuntimeError):
    """Levee quand un controle bloquant renvoie au moins une ligne."""


def clean(connection) -> dict[str, int]:
    """Applique les regles de nettoyage et renvoie le nombre de lignes corrigees."""
    corrections: dict[str, int] = {}
    statements = db.parse_named_statements(SQL_DIR / "20_nettoyage.sql")
    with connection.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement.sql)
            corrections[statement.name] = cursor.rowcount
            LOGGER.info("  %-32s %6d ligne(s) corrigee(s)  (%s)",
                        statement.name, cursor.rowcount, statement.description)
    connection.commit()
    return corrections


def run_checks(connection) -> dict[str, int]:
    """Execute les controles ; leve QualityCheckFailed si un bloquant remonte des lignes."""
    results: dict[str, int] = {}
    failures: list[str] = []
    statements = db.parse_named_statements(SQL_DIR / "21_controles.sql")

    with connection.cursor() as cursor:
        for check in statements:
            cursor.execute(check.sql)
            rows = cursor.fetchall()
            results[check.name] = len(rows)

            if check.severity == "info":
                LOGGER.info("  [info]     %-38s %6d ligne(s)  (%s)",
                            check.name, len(rows), check.description)
                continue

            if rows:
                LOGGER.error("  [ECHEC]    %-38s %6d ligne(s)  (%s)",
                             check.name, len(rows), check.description)
                for row in rows[:MAX_SAMPLE_ROWS]:
                    LOGGER.error("             exemple : %s", row)
                failures.append(check.name)
            else:
                LOGGER.info("  [ok]       %-38s      0 ligne   (%s)",
                            check.name, check.description)

    if failures:
        raise QualityCheckFailed(
            "Controles bloquants en echec : " + ", ".join(failures)
        )
    return results


def main() -> None:
    with db.connect() as connection:
        with step(LOGGER, "nettoyage des donnees"):
            corrections = clean(connection)
        LOGGER.info("Total corrige : %d ligne(s)", sum(corrections.values()))

        with step(LOGGER, "controles de qualite"):
            run_checks(connection)
    LOGGER.info("Donnees conformes, extraction possible")


if __name__ == "__main__":
    main()
