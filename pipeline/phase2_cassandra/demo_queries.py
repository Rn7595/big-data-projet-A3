"""Execution des quatre requetes metier qui ont dicte le modele.

Ce script n'est pas un test : c'est la demonstration que chaque table repond a
la requete pour laquelle elle a ete concue, en interrogeant une partition et
une seule. Chaque requete affichee comporte sa cle de partition complete dans
sa clause WHERE -- c'est le critere qui distingue une lecture Cassandra saine
d'un balayage du cluster.
"""

from __future__ import annotations

from pipeline.config import CASSANDRA
from pipeline.phase2_cassandra import session as cass
from pipeline.utils import get_logger

LOGGER = get_logger("phase2.requetes")


def titre(numero: str, question: str, requete: str) -> None:
    print(f"\n--- {numero} : {question}")
    print(f"    {requete}")


def main() -> None:
    cluster, session = cass.connect()
    try:
        cass.register_types(cluster, CASSANDRA.keyspace)
        session.set_keyspace(CASSANDRA.keyspace)

        # Q1 -----------------------------------------------------------------
        # Le client est choisi au hasard parmi les commandes chargees.
        customer_id = session.execute(
            "SELECT customer_id FROM orders_by_customer LIMIT 1").one().customer_id
        titre("Q1", "les dernieres commandes d'un client",
              f"SELECT ... FROM orders_by_customer WHERE customer_id = {customer_id} LIMIT 5")
        rows = session.execute(
            "SELECT order_ref, order_date, order_status, items_count, total_amount "
            "FROM orders_by_customer WHERE customer_id = %s LIMIT 5", (customer_id,))
        for row in rows:
            print(f"      {row.order_ref}  {row.order_date:%Y-%m-%d}  {row.order_status:<10}"
                  f"  {row.items_count} article(s)  {row.total_amount} EUR")
        print("    Les lignes sortent deja triees du plus recent au plus ancien :")
        print("    l'ordre est celui du disque, aucun tri n'est calcule ici.")

        # Q2 -----------------------------------------------------------------
        order_id = session.execute("SELECT order_id FROM order_by_id LIMIT 1").one().order_id
        titre("Q2", "le detail d'une commande",
              f"SELECT ... FROM order_by_id WHERE order_id = {order_id}")
        order = session.execute(
            "SELECT order_ref, customer_email, total_amount, items "
            "FROM order_by_id WHERE order_id = %s", (order_id,)).one()
        print(f"      {order.order_ref}  {order.customer_email}  {order.total_amount} EUR")
        for item in order.items:
            print(f"        ligne {item.line_no}  {item.product_name[:42]:<42}"
                  f"  x{item.quantity}  {item.line_amount} EUR")
        print("    Les lignes de commande sont dans la ligne elle-meme :")
        print("    une seule lecture, une seule partition, zero jointure.")

        # Q3 -----------------------------------------------------------------
        sample = session.execute(
            "SELECT category_id, year_month FROM sales_by_category_month LIMIT 1").one()
        titre("Q3", "le chiffre d'affaires d'une categorie sur un mois",
              f"SELECT SUM(line_amount) FROM sales_by_category_month "
              f"WHERE category_id = {sample.category_id} AND year_month = '{sample.year_month}'")
        result = session.execute(
            "SELECT COUNT(*) AS lignes, SUM(line_amount) AS chiffre_affaires, "
            "SUM(quantity) AS quantite FROM sales_by_category_month "
            "WHERE category_id = %s AND year_month = %s",
            (sample.category_id, sample.year_month)).one()
        print(f"      {result.lignes} lignes, {result.quantite} articles, "
              f"{result.chiffre_affaires} EUR")
        print("    La cle de partition composite (categorie, mois) est fournie en entier :")
        print("    l'agregation porte sur une partition bornee, pas sur le cluster.")

        # Q4 -----------------------------------------------------------------
        titre("Q4", "le catalogue d'une categorie, par ordre alphabetique",
              f"SELECT ... FROM products_by_category "
              f"WHERE category_id = {sample.category_id} LIMIT 8")
        rows = session.execute(
            "SELECT product_name, brand, unit_price FROM products_by_category "
            "WHERE category_id = %s LIMIT 8", (sample.category_id,))
        for row in rows:
            print(f"      {row.product_name[:46]:<46}  {row.brand:<10}  {row.unit_price} EUR")
        print("    Tri alphabetique obtenu par la cle de clustering, sans ORDER BY.")
        print()
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
