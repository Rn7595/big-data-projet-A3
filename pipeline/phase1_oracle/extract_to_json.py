"""Extraction des documents JSON denormalises depuis Oracle.

Format de sortie : JSON Lines (un document complet par ligne, extension
.jsonl). Ce choix est structurant pour la suite du pipeline :

  * il est ecrit et relu en flux. Un tableau JSON unique obligerait le
    chargeur Cassandra a charger l'integralite du fichier en memoire avant de
    pouvoir traiter le premier document ;
  * il se decoupe trivialement : la ligne est l'unite. C'est le format que
    Spark lit nativement en parallele, un bloc par tache ;
  * il resiste a l'interruption : un fichier tronque reste exploitable jusqu'a
    sa derniere ligne complete.

Le fichier produit est le livrable de l'etape 2 du sujet et la frontiere entre
les deux SGBD : une fois ecrit, Oracle peut etre eteint. Cassandra ne se
connectera jamais a Oracle, il ne lira que ce fichier.
"""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.config import JSON_DIR, REPORT_DIR, SQL_DIR, ensure_dirs
from pipeline.phase1_oracle import db
from pipeline.utils import get_logger, human_size, step

LOGGER = get_logger("phase1.extraction")

EXTRACTIONS = [
    ("commandes denormalisees", "10_extract_orders.sql", "orders.jsonl"),
    ("catalogue produit", "11_extract_products.sql", "products.jsonl"),
]


def extract(connection, sql_file: str, output_file: Path) -> tuple[int, int]:
    """Ecrit un document JSON par ligne et renvoie (nb documents, taille en octets)."""
    query = db.read_query(SQL_DIR / sql_file)
    written = 0
    with output_file.open("w", encoding="utf-8") as handle:
        # La requete renvoie (cle, document) : la cle sert uniquement au tri et
        # aux journaux, seul le document est ecrit.
        for _, document in db.iter_query(connection, query):
            handle.write(document)
            handle.write("\n")
            written += 1
            if written % 10_000 == 0:
                LOGGER.info("    %d documents ecrits", written)
    return written, output_file.stat().st_size


def main() -> None:
    ensure_dirs()
    report: dict[str, dict[str, int]] = {}

    with db.connect() as connection:
        for label, sql_file, filename in EXTRACTIONS:
            output_file = JSON_DIR / filename
            with step(LOGGER, f"extraction : {label}"):
                count, size = extract(connection, sql_file, output_file)
            LOGGER.info("    %s : %d documents, %s", filename, count, human_size(size))
            report[filename] = {"documents": count, "octets": size}

        report["oracle_row_counts"] = db.table_counts(connection)

    # Ce rapport sert de reference aux controles de coherence des phases
    # suivantes : le nombre de documents ecrits ici doit se retrouver a
    # l'identique dans Cassandra, dans Parquet puis dans Elasticsearch.
    report_file = REPORT_DIR / "phase1_extraction.json"
    report_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    LOGGER.info("Rapport ecrit dans %s", report_file)


if __name__ == "__main__":
    main()
