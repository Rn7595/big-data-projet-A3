"""Lecture de Cassandra par Spark, transformations, ecriture en Parquet.

Sortie : une table de faits au grain de la ligne de commande, trois agregats
metier et une segmentation client. Ces fichiers alimentent la phase 4 ; une
fois ecrits, Cassandra peut etre eteint.
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame

from pipeline.config import PARQUET_DIR, REPORT_DIR, ensure_dirs
from pipeline.phase3_spark import transforms as T
from pipeline.phase3_spark.spark_session import build_session, read_cassandra
from pipeline.utils import get_logger, human_size, step

LOGGER = get_logger("phase3.parquet")

SOURCE_TABLE = "sales_by_category_month"


def directory_size(path: Path) -> tuple[int, int]:
    """Renvoie (nombre de fichiers .parquet, taille totale en octets)."""
    files = [f for f in path.rglob("*.parquet") if f.is_file()]
    return len(files), sum(f.stat().st_size for f in files)


def write_parquet(df: DataFrame, name: str, partition_by: list[str] | None = None) -> dict:
    """Ecrit un DataFrame en Parquet et renvoie ses metriques.

    Compression snappy : elle decompresse beaucoup plus vite que gzip pour un
    taux a peine moindre. Sur un format concu pour etre relu souvent, c'est le
    bon arbitrage.
    """
    target = PARQUET_DIR / name
    writer = df.write.mode("overwrite").option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(str(target))

    rows = df.count()
    files, size = directory_size(target)
    LOGGER.info("    %-24s %8d lignes  %3d fichier(s)  %s", name, rows, files, human_size(size))
    return {"lignes": rows, "fichiers": files, "octets": size}


def main() -> None:
    ensure_dirs()
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    report: dict[str, dict] = {}

    try:
        with step(LOGGER, f"lecture de la table Cassandra {SOURCE_TABLE}"):
            raw = read_cassandra(spark, SOURCE_TABLE)
            # La table de faits est relue par cinq traitements successifs.
            # Sans mise en cache, Spark rejouerait la lecture Cassandra a
            # chaque fois : le cache transforme cinq parcours reseau en un seul.
            facts = T.build_fact_order_items(raw).cache()
            nb_lignes = facts.count()
            LOGGER.info("    %d lignes de commande lues", nb_lignes)

        with step(LOGGER, "ecriture de la table de faits"):
            # Partitionnement par annee puis par mois : Spark ecrit un
            # repertoire par couple, et une requete filtrant sur une periode ne
            # lit que les repertoires concernes. C'est le "partition pruning",
            # le principal levier de performance du stockage colonnaire.
            report["fact_order_items"] = write_parquet(
                facts, "fact_order_items", partition_by=["order_year", "order_month"])

        with step(LOGGER, "agregats metier"):
            report["agg_sales_by_month"] = write_parquet(
                T.aggregate_by_month(facts), "agg_sales_by_month")
            report["agg_sales_by_category"] = write_parquet(
                T.aggregate_by_category(facts), "agg_sales_by_category")
            report["agg_top_products"] = write_parquet(
                T.top_products(facts), "agg_top_products")

        with step(LOGGER, "segmentation RFM des clients"):
            rfm = T.customers_rfm(facts)
            report["dim_customers_rfm"] = write_parquet(rfm, "dim_customers_rfm")

        LOGGER.info("Repartition des segments clients :")
        for row in rfm.groupBy("segment").count().orderBy("segment").collect():
            LOGGER.info("    %-16s %6d client(s)", row["segment"], row["count"])

        LOGGER.info("Chiffre d'affaires par annee :")
        annees = (facts.filter("is_revenue")
                  .groupBy("order_year").sum("net_amount")
                  .orderBy("order_year").collect())
        total_ca = 0.0
        for row in annees:
            LOGGER.info("    %s  %14s EUR", row["order_year"], f"{row['sum(net_amount)']:,.2f}")
            total_ca += float(row["sum(net_amount)"])
        # Conserve pour le controle de coherence : Elasticsearch recalculera ce
        # meme total en phase 4, par un chemin entierement different.
        report["chiffre_affaires"] = round(total_ca, 2)
        LOGGER.info("Chiffre d'affaires total : %s EUR", f"{total_ca:,.2f}")

    finally:
        spark.stop()

    # Comparaison des formats : c'est le chiffre a montrer en soutenance pour
    # justifier le passage au colonnaire.
    # Le rapport melange des sections decrivant un fichier et des mesures
    # scalaires : seules les premieres portent une taille.
    total_parquet = sum(section["octets"] for section in report.values()
                        if isinstance(section, dict) and "octets" in section)
    phase1_file = REPORT_DIR / "phase1_extraction.json"
    if phase1_file.exists():
        phase1 = json.loads(phase1_file.read_text(encoding="utf-8"))
        json_size = phase1["orders.jsonl"]["octets"]
        report["comparaison_formats"] = {
            "json_octets": json_size,
            "parquet_octets": total_parquet,
            "ratio": round(json_size / total_parquet, 2) if total_parquet else None,
        }
        LOGGER.info("JSON %s  ->  Parquet %s  (facteur %.1f)",
                    human_size(json_size), human_size(total_parquet),
                    json_size / total_parquet if total_parquet else 0)

    report_file = REPORT_DIR / "phase3_parquet.json"
    report_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    LOGGER.info("Rapport ecrit dans %s", report_file)


if __name__ == "__main__":
    main()
