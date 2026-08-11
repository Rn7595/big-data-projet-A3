"""Fonctions de transformation et d'analyse.

Chaque fonction prend un DataFrame et en renvoie un autre, sans effet de bord :
elles sont ainsi enchainables, testables une par une, et le script principal se
lit comme la description du pipeline plutot que comme son implementation.

C'est la partie "formatage et analyse" demandee par le sujet.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Statuts qui constituent du chiffre d'affaires reel.
#
# Regle metier a assumer et a savoir defendre : une commande annulee ou
# retournee n'a jamais produit de recette, et une commande en attente n'est pas
# encore payee. Les compter gonflerait le chiffre d'affaires d'environ 17 %
# dans ce jeu de donnees. Les lignes correspondantes ne sont pas supprimees
# pour autant : elles restent dans la table de faits, marquees par une colonne
# booleenne, afin de pouvoir analyser le taux d'annulation.
REVENUE_STATUSES = ["PAID", "SHIPPED", "DELIVERED"]

MONEY = "decimal(14,2)"


def enrich_dates(df: DataFrame) -> DataFrame:
    """Derive les colonnes calendaires depuis l'horodatage de commande.

    Ces colonnes n'existent pas dans Cassandra : les y stocker aurait impose de
    les ecrire pour chaque ligne. Les deriver ici coute un balayage unique et
    donne a Kibana des axes d'analyse directement exploitables.
    """
    return (
        df
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_day", F.dayofmonth("order_date"))
        .withColumn("order_dow", F.date_format("order_date", "EEEE"))
        .withColumn("order_hour", F.hour("order_date"))
        .withColumn("order_date_day", F.to_date("order_date"))
    )


def flag_revenue(df: DataFrame) -> DataFrame:
    """Marque les lignes constituant du chiffre d'affaires et calcule le montant net."""
    is_revenue = F.col("order_status").isin(REVENUE_STATUSES)
    return (
        df
        .withColumn("is_revenue", is_revenue)
        .withColumn(
            "net_amount",
            F.when(is_revenue, F.col("line_amount")).otherwise(F.lit(0)).cast(MONEY),
        )
    )


def build_fact_order_items(df: DataFrame) -> DataFrame:
    """Construit la table de faits au grain de la ligne de commande."""
    columns = [
        "order_id", "line_no", "order_date", "order_date_day", "order_year",
        "order_month", "order_day", "order_dow", "order_hour", "year_month",
        "order_status", "is_revenue", "customer_id", "loyalty_tier",
        "category_id", "category_name", "parent_category_id", "parent_category_name",
        "product_id", "sku", "product_name", "brand",
        "quantity", "unit_price", "discount_pct", "line_amount", "net_amount",
        "country_code", "country_name", "region", "city",
    ]
    return (
        df
        .transform(enrich_dates)
        .transform(flag_revenue)
        .withColumn("unit_price", F.col("unit_price").cast(MONEY))
        .withColumn("line_amount", F.col("line_amount").cast(MONEY))
        .select(*columns)
    )


def aggregate_by_month(facts: DataFrame) -> DataFrame:
    """Chiffre d'affaires, commandes et panier moyen par mois."""
    return (
        facts
        .groupBy("year_month", "order_year", "order_month")
        .agg(
            F.sum("net_amount").cast(MONEY).alias("chiffre_affaires"),
            F.countDistinct("order_id").alias("nb_commandes"),
            F.sum("quantity").alias("nb_articles"),
            F.countDistinct("customer_id").alias("nb_clients"),
            F.sum(F.when(~F.col("is_revenue"), 1).otherwise(0)).alias("nb_lignes_annulees"),
        )
        .withColumn(
            "panier_moyen",
            (F.col("chiffre_affaires") / F.col("nb_commandes")).cast(MONEY),
        )
        .orderBy("year_month")
    )


def aggregate_by_category(facts: DataFrame) -> DataFrame:
    """Chiffre d'affaires par categorie et par mois, avec la part du rayon.

    La fonction de fenetrage evite un second passage sur les donnees : la part
    de chaque categorie dans son rayon est calculee dans le meme plan
    d'execution que l'agregation elle-meme.
    """
    par_rayon = Window.partitionBy("year_month", "parent_category_id")
    return (
        facts
        .groupBy("year_month", "parent_category_id", "parent_category_name",
                 "category_id", "category_name")
        .agg(
            F.sum("net_amount").cast(MONEY).alias("chiffre_affaires"),
            F.sum("quantity").alias("nb_articles"),
            F.countDistinct("order_id").alias("nb_commandes"),
        )
        .withColumn("ca_rayon", F.sum("chiffre_affaires").over(par_rayon).cast(MONEY))
        .withColumn(
            "part_dans_rayon_pct",
            F.round(100 * F.col("chiffre_affaires") / F.col("ca_rayon"), 2),
        )
        .orderBy("year_month", F.desc("chiffre_affaires"))
    )


def top_products(facts: DataFrame, top_n: int = 50) -> DataFrame:
    """Les N produits les plus vendus par rayon, par chiffre d'affaires."""
    classement = Window.partitionBy("parent_category_id").orderBy(F.desc("chiffre_affaires"))
    return (
        facts
        .filter(F.col("is_revenue"))
        .groupBy("parent_category_id", "parent_category_name", "category_name",
                 "product_id", "sku", "product_name", "brand")
        .agg(
            F.sum("net_amount").cast(MONEY).alias("chiffre_affaires"),
            F.sum("quantity").alias("quantite_vendue"),
            F.countDistinct("order_id").alias("nb_commandes"),
            F.avg("discount_pct").cast("decimal(5,2)").alias("remise_moyenne_pct"),
        )
        .withColumn("rang", F.row_number().over(classement))
        .filter(F.col("rang") <= top_n)
        .orderBy("parent_category_id", "rang")
    )


def customers_rfm(facts: DataFrame) -> DataFrame:
    """Segmentation RFM des clients.

    R (recence)   : nombre de jours depuis la derniere commande
    F (frequence) : nombre de commandes distinctes
    M (montant)   : chiffre d'affaires net cumule

    Les trois mesures sont converties en scores de 1 a 5 par quintiles, avec
    `ntile`. Un decoupage par quintiles plutot que par seuils fixes rend la
    segmentation independante de la devise, du volume et de la periode : elle
    reste valable si le jeu de donnees change d'echelle.

    La recence est inversee : une petite recence est un bon signe, elle doit
    donc donner un score eleve.

    La date de reference est la derniere commande observee, et non la date du
    jour : sinon la segmentation vieillirait toute seule entre le calcul et la
    lecture du tableau de bord.
    """
    reference_date = facts.agg(F.max("order_date_day")).collect()[0][0]

    # Le regroupement porte sur le seul customer_id. Ajouter les attributs
    # descriptifs (fidelite, pays) a la cle de regroupement serait tentant mais
    # dangereux : il suffirait qu'un client ait commande depuis deux pays pour
    # qu'il apparaisse en deux lignes, avec une segmentation calculee sur des
    # achats fractionnes. On garantit ici une ligne par client, et les attributs
    # descriptifs sont ramenes par agregation.
    base = (
        facts
        .filter(F.col("is_revenue"))
        .groupBy("customer_id")
        .agg(
            F.max("loyalty_tier").alias("loyalty_tier"),
            F.max("country_code").alias("country_code"),
            F.max("country_name").alias("country_name"),
            F.max("region").alias("region"),
            F.max("city").alias("city"),
            F.max("order_date_day").alias("derniere_commande"),
            F.min("order_date_day").alias("premiere_commande"),
            F.countDistinct("order_id").alias("frequence"),
            F.sum("net_amount").cast(MONEY).alias("montant"),
            F.sum("quantity").alias("nb_articles"),
        )
        .withColumn("recence_jours", F.datediff(F.lit(reference_date), F.col("derniere_commande")))
        .withColumn("panier_moyen", (F.col("montant") / F.col("frequence")).cast(MONEY))
    )

    # Fenetres sans partitionBy : le classement porte sur l'ensemble des
    # clients. Acceptable ici (quelques milliers de lignes) ; sur un volume
    # bien superieur, on passerait par approxQuantile pour eviter de ramener
    # toutes les lignes sur une seule partition.
    ordre_recence = Window.orderBy(F.asc("recence_jours"))
    ordre_frequence = Window.orderBy(F.asc("frequence"))
    ordre_montant = Window.orderBy(F.asc("montant"))

    scored = (
        base
        .withColumn("score_recence", F.ntile(5).over(ordre_recence))
        .withColumn("score_frequence", F.ntile(5).over(ordre_frequence))
        .withColumn("score_montant", F.ntile(5).over(ordre_montant))
    )
    # ntile(5) attribue 1 au plus petit ; pour la recence, le plus petit nombre
    # de jours est le meilleur client, d'ou l'inversion.
    scored = scored.withColumn("score_recence", F.lit(6) - F.col("score_recence"))

    score_total = F.col("score_recence") + F.col("score_frequence") + F.col("score_montant")
    segment = (
        F.when((F.col("score_recence") >= 4) & (F.col("score_frequence") >= 4) &
               (F.col("score_montant") >= 4), "Champions")
        .when((F.col("score_recence") >= 3) & (F.col("score_frequence") >= 3), "Fideles")
        .when((F.col("score_recence") >= 4) & (F.col("score_frequence") <= 2), "Nouveaux")
        .when((F.col("score_recence") <= 2) & (F.col("score_montant") >= 4), "A reconquerir")
        .when(F.col("score_recence") <= 2, "Endormis")
        .otherwise("A surveiller")
    )

    return (
        scored
        .withColumn("score_rfm", score_total)
        .withColumn("segment", segment)
        .orderBy(F.desc("montant"))
    )
