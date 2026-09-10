"""Verifie les definitions des indicateurs avec de petits DataFrames Spark."""

import unittest
from datetime import date
from decimal import Decimal

from pyspark.sql import SparkSession, functions as F

from pipeline.phase3_spark import transforms as T


class IndicateursTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("tests-indicateurs")
                     .config("spark.ui.enabled", "false")
                     .config("spark.ui.showConsoleProgress", "false")
                     .config("spark.driver.host", "127.0.0.1")
                     .config("spark.sql.shuffle.partitions", "2")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def faits(self):
        lignes = [(1, "PAID", "10.00"), (1, "PAID", "30.00"),
                  (2, "CANCELLED", "90.00"), (3, "PENDING", "80.00"),
                  (4, "RETURNED", "70.00"), (5, "SHIPPED", "60.00")]
        df = self.spark.createDataFrame(
            [(identifiant, statut, Decimal(prix)) for identifiant, statut, prix in lignes],
            "order_id long, order_status string, line_amount decimal(14,2)")
        return (T.flag_revenue(df)
                .withColumn("year_month", F.lit("2026-09"))
                .withColumn("order_year", F.lit(2026))
                .withColumn("order_month", F.lit(9))
                .withColumn("quantity", F.lit(1))
                .withColumn("customer_id", F.col("order_id")))

    def test_moyenne_tous_statuts_et_lignes_sans_ca(self):
        ligne = T.aggregate_by_month(self.faits()).first().asDict()
        self.assertEqual(ligne["chiffre_affaires"], Decimal("100.00"))
        self.assertEqual(ligne["nb_commandes"], 5)
        self.assertEqual(ligne["nb_articles"], 6)
        self.assertEqual(ligne["nb_lignes_sans_ca"], 3)
        self.assertEqual(ligne["ca_moyen_par_commande"], Decimal("20.00"))
        self.assertNotIn("nb_lignes_annulees", ligne)
        self.assertNotIn("panier_moyen", ligne)

    def test_panier_rfm_reste_limite_aux_commandes_avec_ca(self):
        faits = self.faits().withColumn("order_date_day", F.lit(date(2026, 9, 1)))
        for colonne, valeur in {"loyalty_tier": "SILVER", "country_code": "FR",
                                "country_name": "France", "region": "Europe",
                                "city": "Paris"}.items():
            faits = faits.withColumn(colonne, F.lit(valeur))
        clients = {ligne.customer_id: ligne for ligne in T.customers_rfm(faits).collect()}
        self.assertEqual(set(clients), {1, 5})
        self.assertEqual(clients[1].frequence, 1)
        self.assertEqual(clients[1].panier_moyen, Decimal("40.00"))
        self.assertEqual(clients[5].panier_moyen, Decimal("60.00"))


if __name__ == "__main__":
    unittest.main()
