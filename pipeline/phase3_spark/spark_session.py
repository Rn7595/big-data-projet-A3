"""Construction de la session Spark et lecture de Cassandra.

Spark s'execute en local (`local[*]`), sans conteneur : le pilote et les
executeurs vivent dans le meme processus JVM. Pour un jeu de cette taille, un
cluster serait du folklore -- le cout de coordination depasserait le gain. Le
code reste pourtant identique a celui d'un cluster : seule l'URL du maitre
changerait.

Le connecteur Cassandra est charge par coordonnees Maven plutot que par un jar
depose dans le depot : la version est ainsi declaree dans le code, et le jar
n'alourdit pas le livrable. Il est telecharge au premier lancement, puis mis en
cache dans ~/.ivy2.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from pipeline.config import CASSANDRA
from pipeline.utils import get_logger

LOGGER = get_logger("phase3.spark")

# Le suffixe _2.12 est la version de Scala avec laquelle Spark 3.5 est compile.
# Une version de connecteur qui ne correspondrait pas produirait des erreurs de
# methode introuvable a l'execution, sans message explicite.
CONNECTOR = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"


def build_session(app_name: str = "pipeline-ecommerce") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", CONNECTOR)
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
        .config("spark.cassandra.connection.host", CASSANDRA.host)
        .config("spark.cassandra.connection.port", str(CASSANDRA.port))
        # 4 partitions de sortie : sans ce reglage, Spark en produirait 200 par
        # defaut apres chaque agregation, soit des centaines de fichiers Parquet
        # de quelques kilo-octets. Beaucoup de petits fichiers est le probleme
        # classique du stockage colonnaire.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def read_cassandra(spark: SparkSession, table: str) -> DataFrame:
    """Lit une table Cassandra comme un DataFrame.

    Le connecteur decoupe la lecture en taches suivant les plages de jetons de
    l'anneau : chaque tache lit un segment de l'espace des cles de partition.
    C'est ce qui permettrait a la lecture de passer a l'echelle sur un vrai
    cluster, chaque executeur lisant les donnees du noeud dont il est le plus
    proche.
    """
    LOGGER.info("Lecture de %s.%s", CASSANDRA.keyspace, table)
    return (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=CASSANDRA.keyspace)
        .load()
    )
