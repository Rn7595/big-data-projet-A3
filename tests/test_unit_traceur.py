"""Regressions du traceur, sans demarrer de base de donnees."""

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from tests import tracer_commande as traceur


def source(total="100.00", lignes=2, **champs):
    return {"total": Decimal(total), "lignes": lignes, **champs}


class ComparaisonTests(unittest.TestCase):
    def test_frais_de_port_explicites_ne_faussent_pas_la_comparaison(self):
        resultats = {
            "JSON": source(total_commande=Decimal("104.90"), frais_port=Decimal("4.90")),
            "Parquet": source(net=Decimal("100.00")),
            "Elasticsearch": source(net=Decimal("100.00")),
        }
        self.assertEqual(traceur.comparer_sources(resultats), (0, []))

    def test_ecarts_inferieurs_a_cinq_euros_sont_rejetes(self):
        for total in ("100.01", "104.00", "104.90", "105.00"):
            with self.subTest(total=total):
                code, erreurs = traceur.comparer_sources({
                    "Parquet": source(), "Elasticsearch": source(total),
                })
                self.assertEqual(code, 1)
                self.assertIn("articles hors port", " ".join(erreurs))

    def test_meme_montant_avec_ligne_manquante_est_rejete(self):
        code, erreurs = traceur.comparer_sources({
            "Parquet": source(), "Elasticsearch": source(lignes=1),
        })
        self.assertEqual(code, 1)
        self.assertIn("nombre de lignes", " ".join(erreurs))

    def test_total_stocke_incoherent_est_rejete(self):
        code, erreurs = traceur.comparer_sources({
            "JSON": source(total_commande=Decimal("104.91"), frais_port=Decimal("4.90")),
            "Parquet": source(),
        })
        self.assertEqual(code, 1)
        self.assertIn("total stocke", " ".join(erreurs))

    def test_nombre_de_lignes_annonce_incoherent_est_rejete(self):
        code, _ = traceur.comparer_sources({
            "Cassandra": source(lignes_annoncees=3), "Parquet": source(),
        })
        self.assertEqual(code, 1)

    def test_ecart_de_frais_de_port_est_rejete(self):
        code, _ = traceur.comparer_sources({
            "Oracle": source(frais_port=Decimal("4.90")),
            "Cassandra": source(frais_port=Decimal("0.00")),
        })
        self.assertEqual(code, 1)

    def test_ecart_net_est_rejete_meme_si_les_articles_coincident(self):
        code, erreurs = traceur.comparer_sources({
            "Parquet": source(net=Decimal("0.00")),
            "Elasticsearch": source(net=Decimal("100.00")),
        })
        self.assertEqual(code, 1)
        self.assertIn("montant net", " ".join(erreurs))

    def test_absence_de_commande_differe_d_une_source_eteinte(self):
        autres = {"JSON": source(), "Parquet": source()}
        self.assertEqual(traceur.comparer_sources({**autres, "Oracle": None})[0], 1)
        self.assertEqual(traceur.comparer_sources({**autres, "Oracle": {"erreur": "eteint"}})[0], 0)

    def test_comparaison_impossible_ne_reussit_pas(self):
        for resultats in ({}, {"JSON": None}, {"JSON": source()},
                          {"JSON": source(), "Oracle": {"erreur": "eteint"}}):
            with self.subTest(resultats=resultats):
                self.assertEqual(traceur.comparer_sources(resultats)[0], 2)

    def test_bruit_flottant_ne_cree_pas_un_faux_ecart(self):
        resultats = {"Parquet": source(), "Elasticsearch": source("100.00000000001")}
        self.assertEqual(traceur.comparer_sources(resultats), (0, []))

    def test_sortie_annonce_une_comparaison_partielle(self):
        sources = [("JSON", lambda _: source()), ("Parquet", lambda _: source()),
                   ("Oracle", lambda _: {"erreur": "eteint"})]
        sortie = io.StringIO()
        with patch.object(traceur, "SOURCES", sources), \
                patch.object(traceur.sys, "argv", ["traceur", "1"]), redirect_stdout(sortie):
            self.assertEqual(traceur.main(), 0)
        self.assertIn("Comparaison partielle : 2/3", sortie.getvalue())


class LectureFichiersTests(unittest.TestCase):
    def test_json_et_parquet_comparent_les_articles_sans_les_frais(self):
        with tempfile.TemporaryDirectory() as dossier:
            racine = Path(dossier)
            document = {"order_id": 7, "items_count": 2,
                        "items": [{"line_amount": 12.35}, {"line_amount": 0.15}],
                        "total_amount": 17.40, "shipping_amount": 4.90,
                        "order_status": "PAID"}
            (racine / "orders.jsonl").write_text(json.dumps(document) + "\n", encoding="utf-8")
            faits = racine / "fact_order_items"
            faits.mkdir()
            table = pa.table({
                "order_id": [7, 7, 8],
                "line_amount": [Decimal("12.35"), Decimal("0.15"), Decimal("999.00")],
                "net_amount": [Decimal("12.35"), Decimal("0.15"), Decimal("999.00")],
                "order_status": ["PAID", "PAID", "DELIVERED"],
            })
            pq.write_table(table, faits / "part.parquet")
            with patch.object(traceur, "JSON_DIR", racine), patch.object(traceur, "PARQUET_DIR", racine):
                resultats = {"JSON": traceur.depuis_json(7), "Parquet": traceur.depuis_parquet(7)}
                self.assertEqual(traceur.depuis_json(404), None)
                self.assertEqual(traceur.depuis_parquet(404), None)
            self.assertEqual(resultats["JSON"]["total"], Decimal("12.50"))
            self.assertEqual(resultats["Parquet"]["lignes"], 2)
            self.assertEqual(traceur.comparer_sources(resultats), (0, []))


if __name__ == "__main__":
    unittest.main()
