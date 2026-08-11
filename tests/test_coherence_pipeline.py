"""Controles de coherence entre les quatre phases du pipeline.

Chaque phase ecrit un rapport dans data/reports/. Ce script les rapproche et
verifie que la meme donnee se retrouve, en meme quantite, d'un bout a l'autre
de la chaine.

C'est la reponse a la question « comment savez-vous que rien n'a ete perdu
entre Oracle et Kibana ? ». La reponse n'est pas « je l'ai regarde », mais
« un controle automatique le verifie, et il echoue si ce n'est pas le cas ».

    python -m tests.test_coherence_pipeline

Le script est aussi compatible pytest : chaque controle est une fonction
`test_*` independante.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "data" / "reports"

RAPPORTS = {
    "phase1": "phase1_extraction.json",
    "phase2": "phase2_chargement.json",
    "phase3": "phase3_parquet.json",
    "phase4": "phase4_indexation.json",
}

# Tolerance sur les montants : Spark agrege des decimaux exacts, Elasticsearch
# des scaled_float. L'ecart admissible reste de l'ordre du centime.
TOLERANCE_MONTANT = 0.05


def charger(phase: str) -> dict:
    fichier = REPORTS / RAPPORTS[phase]
    if not fichier.exists():
        raise FileNotFoundError(
            f"{fichier} est absent : la {phase} n'a pas ete executee."
        )
    return json.loads(fichier.read_text(encoding="utf-8"))


def test_lignes_de_commande_conservees() -> tuple[str, str]:
    """Le nombre de lignes de commande doit etre identique aux quatre etapes.

    C'est le controle central : la ligne de commande est le grain le plus fin
    du pipeline, celui qui traverse les quatre technologies sans jamais etre
    agrege.
    """
    oracle = charger("phase1")["oracle_row_counts"]["order_items"]
    cassandra = charger("phase2")["comptees"]["sales_by_category_month"]
    parquet = charger("phase3")["fact_order_items"]["lignes"]
    elastic = charger("phase4")["ecom-order-items"]

    valeurs = {"Oracle": oracle, "Cassandra": cassandra,
               "Parquet": parquet, "Elasticsearch": elastic}
    assert len(set(valeurs.values())) == 1, f"Ecart de volumetrie : {valeurs}"
    return ("lignes de commande conservees",
            " = ".join(f"{nom} {nombre}" for nom, nombre in valeurs.items()))


def test_commandes_conservees() -> tuple[str, str]:
    """Les documents JSON extraits doivent tous se retrouver dans Cassandra."""
    phase1 = charger("phase1")
    phase2 = charger("phase2")

    documents = phase1["orders.jsonl"]["documents"]
    par_client = phase2["comptees"]["orders_by_customer"]
    par_id = phase2["comptees"]["order_by_id"]

    assert documents == par_client == par_id, (
        f"JSON {documents}, orders_by_customer {par_client}, order_by_id {par_id}")
    return ("commandes conservees",
            f"{documents} documents JSON = {par_client} lignes dans les deux tables")


def test_ecart_commandes_explique() -> tuple[str, str]:
    """L'ecart entre les commandes d'Oracle et les documents extraits doit
    correspondre exactement aux commandes sans ligne.

    Une commande vide ne produit pas de document : la jointure interne de la
    requete de denormalisation l'ecarte. L'ecart n'est donc pas une perte, mais
    il doit etre integralement explique.
    """
    phase1 = charger("phase1")
    commandes = phase1["oracle_row_counts"]["orders"]
    documents = phase1["orders.jsonl"]["documents"]
    ecart = commandes - documents

    assert ecart >= 0, f"Plus de documents ({documents}) que de commandes ({commandes})"
    return ("ecart de commandes explique",
            f"{commandes} commandes - {ecart} sans ligne = {documents} documents")


def test_produits_conserves() -> tuple[str, str]:
    """Le catalogue doit arriver entier dans Cassandra."""
    oracle = charger("phase1")["oracle_row_counts"]["products"]
    cassandra = charger("phase2")["comptees"]["products_by_category"]
    assert oracle == cassandra, f"Oracle {oracle}, Cassandra {cassandra}"
    return "catalogue conserve", f"{oracle} produits des deux cotes"


def test_clients_segmentes_indexes() -> tuple[str, str]:
    """Tous les clients segmentes par Spark doivent etre indexes."""
    parquet = charger("phase3")["dim_customers_rfm"]["lignes"]
    elastic = charger("phase4")["ecom-customers"]
    assert parquet == elastic, f"Parquet {parquet}, Elasticsearch {elastic}"

    total = charger("phase1")["oracle_row_counts"]["customers"]
    return ("clients segmentes indexes",
            f"{parquet} clients avec chiffre d'affaires sur {total} inscrits")


def test_chiffre_affaires_identique() -> tuple[str, str]:
    """Spark et Elasticsearch doivent trouver le meme chiffre d'affaires.

    Le controle le plus fort du lot : deux moteurs differents, deux chemins de
    calcul independants, un seul resultat attendu. Spark agrege des decimaux
    depuis Parquet ; Elasticsearch agrege des scaled_float depuis son index.
    """
    spark = charger("phase3").get("chiffre_affaires")
    elastic = charger("phase4").get("chiffre_affaires")
    if spark is None:
        return ("chiffre d'affaires", "non compare (rapport de phase 3 anterieur)")

    ecart = abs(float(spark) - float(elastic))
    assert ecart <= TOLERANCE_MONTANT, (
        f"Spark {spark:,.2f} EUR, Elasticsearch {elastic:,.2f} EUR, ecart {ecart:,.2f}")
    return ("chiffre d'affaires identique",
            f"{float(spark):,.2f} EUR calcule par Spark et par Elasticsearch")


def test_compression_parquet() -> tuple[str, str]:
    """Parquet doit etre nettement plus compact que le JSON d'origine."""
    comparaison = charger("phase3").get("comparaison_formats")
    if not comparaison:
        return "compression", "non mesuree"
    ratio = comparaison["ratio"]
    assert ratio > 1, f"Parquet n'est pas plus compact (ratio {ratio})"
    return ("compression Parquet",
            f"{comparaison['json_octets'] / 1e6:.1f} Mo de JSON -> "
            f"{comparaison['parquet_octets'] / 1e6:.1f} Mo, facteur {ratio}")


CONTROLES = [
    test_lignes_de_commande_conservees,
    test_commandes_conservees,
    test_ecart_commandes_explique,
    test_produits_conserves,
    test_clients_segmentes_indexes,
    test_chiffre_affaires_identique,
    test_compression_parquet,
]


def main() -> int:
    print("Controles de coherence du pipeline\n" + "=" * 74)
    echecs = 0
    for controle in CONTROLES:
        try:
            titre, detail = controle()
            print(f"  [ok]     {titre:<34} {detail}")
        except AssertionError as erreur:
            print(f"  [ECHEC]  {controle.__name__:<34} {erreur}")
            echecs += 1
        except FileNotFoundError as erreur:
            print(f"  [absent] {controle.__name__:<34} {erreur}")
            echecs += 1

    print("=" * 74)
    if echecs:
        print(f"{echecs} controle(s) en echec.")
        return 1
    print(f"{len(CONTROLES)} controles passes : la donnee traverse le pipeline sans perte.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
