"""Compare une commande a travers les sources disponibles du pipeline.

    python -m tests.tracer_commande 46463

Les montants compares sont ceux des articles apres remise, hors frais de port.
Les totaux stockes dans JSON et Cassandra sont aussi rapproches de leurs
articles et frais de port explicites. Le montant net est compare entre
Parquet et Elasticsearch.

Les sources eteintes sont signalees. Une comparaison partielle avec au moins
deux sources disponibles ne valide pas les cinq sources. Les fichiers JSON et
Parquet doivent exister sur disque.

Codes de sortie : 0 = sources disponibles coherentes ; 1 = incoherence ;
2 = usage invalide ou comparaison impossible faute de sources.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal, ROUND_HALF_UP

from pipeline.config import CASSANDRA, ELASTIC, JSON_DIR, ORACLE, PARQUET_DIR

LARGEUR = 78
CENTIME = Decimal("0.01")


def montant(value) -> Decimal:
    """Compare au centime, en neutralisant uniquement le bruit des flottants."""
    return Decimal(str(value)).quantize(CENTIME, rounding=ROUND_HALF_UP)


def entete(titre: str) -> None:
    print(f"\n{titre}\n" + "-" * LARGEUR)


def depuis_oracle(order_id: int) -> dict | None:
    """Recalcule les articles hors port, arrondis par ligne comme le SQL/JSON."""
    try:
        import oracledb

        with oracledb.connect(user=ORACLE.user, password=ORACLE.password,
                              dsn=ORACLE.dsn, tcp_connect_timeout=5) as connexion:
            with connexion.cursor() as curseur:
                curseur.execute(
                    """
                    SELECT COUNT(*),
                           SUM(ROUND(oi.quantity * oi.unit_price * (1 - oi.discount_pct/100), 2)),
                           MAX(o.shipping_amount)
                    FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
                    WHERE o.order_id = :1
                    """,
                    [order_id],
                )
                lignes, total, frais_port = curseur.fetchone()
        if not lignes:
            return None
        return {"lignes": lignes, "total": montant(total),
                "frais_port": montant(frais_port)}
    except Exception as erreur:  # noqa: BLE001 - indisponible, jamais marque valide
        return {"erreur": str(erreur).split("\n")[0][:60]}


def depuis_json(order_id: int) -> dict | None:
    """Relit les articles, les frais et le total du document extrait."""
    fichier = JSON_DIR / "orders.jsonl"
    if not fichier.exists():
        return {"erreur": "data/json/orders.jsonl absent"}
    with fichier.open(encoding="utf-8") as handle:
        for ligne in handle:
            document = json.loads(ligne, parse_float=Decimal, parse_int=int)
            if document["order_id"] == order_id:
                somme = sum((Decimal(str(item["line_amount"]))
                             for item in document["items"]), Decimal(0))
                return {
                    "lignes": len(document["items"]),
                    "lignes_annoncees": document["items_count"],
                    "total": somme,
                    "total_commande": Decimal(str(document["total_amount"])),
                    "frais_port": Decimal(str(document["shipping_amount"])),
                    "statut": document["order_status"],
                }
    return None


def depuis_cassandra(order_id: int) -> dict | None:
    cluster = None
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA.host], port=CASSANDRA.port, connect_timeout=5)
        session = cluster.connect(CASSANDRA.keyspace)
        ligne = session.execute(
            "SELECT items, items_count, total_amount, shipping_amount, order_status "
            "FROM order_by_id WHERE order_id = %s", (order_id,)).one()
        if ligne is None:
            return None
        return {
            "lignes": len(ligne.items),
            "lignes_annoncees": ligne.items_count,
            "total": sum((item.line_amount for item in ligne.items), Decimal(0)),
            "total_commande": ligne.total_amount,
            "frais_port": ligne.shipping_amount,
            "statut": ligne.order_status,
        }
    except Exception as erreur:  # noqa: BLE001
        return {"erreur": str(erreur).split("\n")[0][:60]}
    finally:
        if cluster is not None:
            cluster.shutdown()


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
            "total": sum(table.column("line_amount").to_pylist(), Decimal(0)),
            "net": sum(table.column("net_amount").to_pylist(), Decimal(0)),
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
                "track_total_hits": True,
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
            "total": montant(corps["aggregations"]["total"]["value"]),
            "net": montant(corps["aggregations"]["net"]["value"]),
        }
    except Exception as erreur:  # noqa: BLE001
        return {"erreur": str(erreur).split("\n")[0][:60]}


SOURCES = [
    ("Oracle (calcul par jointure)", depuis_oracle),
    ("JSON denormalise", depuis_json),
    ("Cassandra (articles et total)", depuis_cassandra),
    ("Parquet", depuis_parquet),
    ("Elasticsearch", depuis_elasticsearch),
]


def comparer_sources(resultats: dict[str, dict | None]) -> tuple[int, list[str]]:
    """Verifie les mesures comparables ; un ecart d'un centime est un echec."""
    disponibles = {nom: valeur for nom, valeur in resultats.items()
                   if valeur is not None and "erreur" not in valeur}
    erreurs = []

    for nom, valeur in disponibles.items():
        if ("lignes_annoncees" in valeur
                and valeur["lignes_annoncees"] != valeur["lignes"]):
            erreurs.append(f"{nom} : nombre de lignes annonce different du contenu.")
        if "total_commande" in valeur:
            attendu = montant(valeur["total"]) + montant(valeur["frais_port"])
            if montant(valeur["total_commande"]) != attendu:
                erreurs.append(f"{nom} : total stocke different des articles + frais de port.")

    if disponibles:
        for nom, valeur in resultats.items():
            if valeur is None:
                erreurs.append(f"{nom} : commande absente alors qu'une autre source la contient.")

    for champ, label in (("lignes", "nombre de lignes"),
                         ("total", "montant des articles hors port"),
                         ("frais_port", "frais de port"),
                         ("net", "montant net")):
        valeurs = {nom: (valeur[champ] if champ == "lignes" else montant(valeur[champ]))
                   for nom, valeur in disponibles.items() if champ in valeur}
        if len(set(valeurs.values())) > 1:
            detail = ", ".join(f"{nom}={valeur}" for nom, valeur in valeurs.items())
            erreurs.append(f"Ecart de {label} : {detail}.")

    if erreurs:
        return 1, erreurs
    if len(disponibles) < 2:
        return 2, ["Moins de deux sources disponibles : comparaison non concluante."]
    return 0, []


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage : python -m tests.tracer_commande <order_id>")
        return 2
    try:
        order_id = int(sys.argv[1])
        if order_id <= 0:
            raise ValueError
    except ValueError:
        print("L'identifiant de commande doit etre un entier positif.")
        return 2

    entete(f"Commande {order_id} a travers le pipeline")
    print("Montants des articles apres remise, hors frais de port (EUR).")
    print(f"{'Source':<32} {'Lignes':>7} {'Articles':>14}   Remarque")
    print("-" * LARGEUR)

    resultats: dict[str, dict | None] = {}
    for nom, lecture in SOURCES:
        try:
            resultat = lecture(order_id)
        except Exception as erreur:  # noqa: BLE001 - lecture impossible, jamais marquee valide
            resultat = {"erreur": str(erreur).split("\n")[0][:60]}
        resultats[nom] = resultat
        if resultat is None:
            print(f"{nom:<32} {'-':>7} {'-':>14}   commande absente")
            continue
        if "erreur" in resultat:
            print(f"{nom:<32} {'-':>7} {'-':>14}   indisponible : {resultat['erreur']}")
            continue

        remarque = resultat.get("statut", "")
        if "frais_port" in resultat:
            remarque += f" port={montant(resultat['frais_port'])}"
        print(f"{nom:<32} {resultat['lignes']:>7} {montant(resultat['total']):>14}   {remarque}")

    print("-" * LARGEUR)
    code, erreurs = comparer_sources(resultats)
    for erreur in erreurs:
        print(f"{'ECHEC' if code == 1 else 'INCOMPLET'} : {erreur}")
    if code:
        return code

    disponibles = [valeur for valeur in resultats.values()
                   if valeur is not None and "erreur" not in valeur]
    portee = "complete" if len(disponibles) == len(SOURCES) else "partielle"
    print(f"Comparaison {portee} : {len(disponibles)}/{len(SOURCES)} sources disponibles.")
    reference = disponibles[0]
    print(f"Nombre de lignes identique ({reference['lignes']}) et montant des articles "
          f"identique ({montant(reference['total'])} EUR) sur ces sources.")
    if sum("net" in valeur for valeur in disponibles) >= 2:
        print("Montant net identique entre Parquet et Elasticsearch.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
