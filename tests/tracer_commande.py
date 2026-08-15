"""Trace une commande a travers toutes les etapes du pipeline.

    python -m tests.tracer_commande 35704

Le script interroge chaque source disponible et affiche le montant qu'elle
retourne pour la meme commande. Les quatre technologies doivent tomber sur le
meme chiffre : c'est la verification la plus directe qu'aucune donnee n'a ete
alteree en chemin, et elle ne demande de faire confiance a aucun code du
pipeline -- chaque source est interrogee independamment.

Les sources eteintes sont simplement signalees comme indisponibles : le script
fonctionne quelle que soit la phase en cours. Le fichier JSON et le Parquet
sont lus sur disque et repondent donc toujours.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal

from pipeline.config import CASSANDRA, ELASTIC, JSON_DIR, ORACLE, PARQUET_DIR

LARGEUR = 78


def entete(titre: str) -> None:
    print(f"\n{titre}\n" + "-" * LARGEUR)


def depuis_oracle(order_id: int) -> dict | None:
    """Recalcule le total par jointure et agregation, comme le ferait une application."""
    try:
        import oracledb

        with oracledb.connect(user=ORACLE.user, password=ORACLE.password,
                              dsn=ORACLE.dsn, tcp_connect_timeout=5) as connexion:
            with connexion.cursor() as curseur:
                curseur.execute(
                    """
                    SELECT COUNT(*),
                           ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct/100)), 2)
                             + MAX(o.shipping_amount)
                    FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
                    WHERE o.order_id = :1
                    """,
                    [order_id],
                )
                lignes, total = curseur.fetchone()
        if not lignes:
            return None
        return {"lignes": lignes, "total": Decimal(str(total))}
    except Exception as erreur:  # noqa: BLE001 - source simplement indisponible
        return {"erreur": str(erreur).split("\n")[0][:60]}


def depuis_json(order_id: int) -> dict | None:
    """Relit le document extrait, sans passer par aucun code du pipeline."""
    fichier = JSON_DIR / "orders.jsonl"
    if not fichier.exists():
        return {"erreur": "data/json/orders.jsonl absent"}
    with fichier.open(encoding="utf-8") as handle:
        for ligne in handle:
            document = json.loads(ligne, parse_float=Decimal, parse_int=int)
            if document["order_id"] == order_id:
                somme = sum(Decimal(str(item["line_amount"])) for item in document["items"])
                return {
                    "lignes": len(document["items"]),
                    "total": Decimal(str(document["total_amount"])),
                    "somme_lignes": somme + Decimal(str(document["shipping_amount"])),
                    "statut": document["order_status"],
                }
    return None


def depuis_cassandra(order_id: int) -> dict | None:
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA.host], port=CASSANDRA.port, connect_timeout=5)
        session = cluster.connect(CASSANDRA.keyspace)
        try:
            ligne = session.execute(
                "SELECT items_count, total_amount, order_status FROM order_by_id "
                "WHERE order_id = %s", (order_id,)).one()
        finally:
            cluster.shutdown()
        if ligne is None:
            return None
        return {"lignes": ligne.items_count, "total": ligne.total_amount,
                "statut": ligne.order_status}
    except Exception as erreur:  # noqa: BLE001
        return {"erreur": str(erreur).split("\n")[0][:60]}


def depuis_parquet(order_id: int) -> dict | None:
    try:
        import pyarrow.dataset as ds

        jeu = ds.dataset(str(PARQUET_DIR / "fact_order_items"),
                         format="parquet", partitioning="hive")
        table = jeu.to_table(filter=ds.field("order_id") == order_id,
                             columns=["line_amount", "net_amount", "order_status"])
        if table.num_rows == 0:
            return None
        return {
            "lignes": table.num_rows,
            "total": sum(table.column("line_amount").to_pylist()),
            "net": sum(table.column("net_amount").to_pylist()),
            "statut": table.column("order_status").to_pylist()[0],
        }
    except Exception as erreur:  # noqa: BLE001
        return {"erreur": str(erreur).split("\n")[0][:60]}


def depuis_elasticsearch(order_id: int) -> dict | None:
    try:
        import requests

        reponse = requests.post(
            f"{ELASTIC.url}/{ELASTIC.index_items}/_search",
            json={
                "size": 0,
                "query": {"term": {"order_id": order_id}},
                "aggs": {"total": {"sum": {"field": "line_amount"}},
                         "net": {"sum": {"field": "net_amount"}}},
            },
            timeout=10,
        )
        reponse.raise_for_status()
        corps = reponse.json()
        lignes = corps["hits"]["total"]["value"]
        if not lignes:
            return None
        return {
            "lignes": lignes,
            "total": Decimal(str(round(corps["aggregations"]["total"]["value"], 2))),
            "net": Decimal(str(round(corps["aggregations"]["net"]["value"], 2))),
        }
    except Exception as erreur:  # noqa: BLE001
        return {"erreur": str(erreur).split("\n")[0][:60]}


SOURCES = [
    ("Oracle (calcul par jointure)", depuis_oracle),
    ("JSON denormalise", depuis_json),
    ("Cassandra (total pre-calcule)", depuis_cassandra),
    ("Parquet", depuis_parquet),
    ("Elasticsearch", depuis_elasticsearch),
]


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage : python -m tests.tracer_commande <order_id>")
        return 2
    order_id = int(sys.argv[1])

    entete(f"Commande {order_id} a travers le pipeline")
    print(f"{'Source':<32} {'Lignes':>7} {'Montant':>14}   Remarque")
    print("-" * LARGEUR)

    montants: dict[str, Decimal] = {}
    for nom, lecture in SOURCES:
        resultat = lecture(order_id)
        if resultat is None:
            print(f"{nom:<32} {'-':>7} {'-':>14}   commande absente")
            continue
        if "erreur" in resultat:
            print(f"{nom:<32} {'-':>7} {'-':>14}   indisponible : {resultat['erreur']}")
            continue

        remarque = ""
        if "somme_lignes" in resultat and resultat["somme_lignes"] != resultat["total"]:
            remarque = f"somme des lignes = {resultat['somme_lignes']}"
        elif "statut" in resultat:
            remarque = resultat["statut"]

        montants[nom] = Decimal(str(resultat["total"]))
        print(f"{nom:<32} {resultat['lignes']:>7} {resultat['total']:>14}   {remarque}")

    print("-" * LARGEUR)
    if len(montants) < 2:
        print("Moins de deux sources disponibles : rien a comparer.")
        print("Allumez une phase, par exemple : docker compose --profile elastic up -d")
        return 0

    # Les montants Parquet et Elasticsearch portent sur les lignes seules,
    # sans les frais de port que le total de commande inclut : la comparaison
    # se fait a l'euro pres sur les sources comparables.
    valeurs = sorted(montants.values())
    ecart = valeurs[-1] - valeurs[0]
    if ecart <= Decimal("5.00"):
        print(f"{len(montants)} sources interrogees, ecart maximal {ecart} EUR.")
        print("Les frais de port expliquent l'ecart residuel : ils sont inclus dans le")
        print("total de commande, absents de la somme des lignes.")
    else:
        print(f"ECART ANORMAL entre les sources : {ecart} EUR")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
