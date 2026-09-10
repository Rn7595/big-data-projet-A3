"""Configuration centralisee du pipeline.

Toutes les phases lisent leurs parametres ici, et uniquement ici : les
identifiants et les chemins ne sont jamais ecrits en dur dans le code metier.
Les valeurs proviennent du fichier .env (voir .env.example) avec un repli sur
des valeurs par defaut coherentes avec docker-compose.yml.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

SQL_DIR = ROOT / "sql"
CQL_DIR = ROOT / "cql"
DATA_DIR = ROOT / "data"
JSON_DIR = DATA_DIR / "json"
PARQUET_DIR = DATA_DIR / "parquet"
REPORT_DIR = DATA_DIR / "reports"


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


@dataclass(frozen=True)
class OracleConfig:
    host: str = os.getenv("ORACLE_HOST", "localhost")
    port: int = _int("ORACLE_PORT", 1521)
    service: str = os.getenv("ORACLE_SERVICE", "FREEPDB1")
    user: str = os.getenv("ORACLE_USER", "ecom")
    password: str = os.getenv("ORACLE_PASSWORD", "ecom")

    @property
    def dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.service}"


@dataclass(frozen=True)
class CassandraConfig:
    host: str = os.getenv("CASSANDRA_HOST", "127.0.0.1")
    port: int = _int("CASSANDRA_PORT", 9042)
    keyspace: str = os.getenv("CASSANDRA_KEYSPACE", "ecommerce")


@dataclass(frozen=True)
class ElasticConfig:
    url: str = os.getenv("ES_HOST", "http://localhost:9200")
    kibana_url: str = os.getenv("KIBANA_URL", "http://localhost:5601")
    index_items: str = os.getenv("ES_INDEX_ITEMS", "ecom-order-items")
    index_customers: str = os.getenv("ES_INDEX_CUSTOMERS", "ecom-customers")


@dataclass(frozen=True)
class DatasetConfig:
    """Volumetrie du jeu de donnees genere.

    La graine rend la generation deterministe : deux executions produisent
    exactement les memes cles techniques, ce qui permet de comparer les
    comptages d'une phase a l'autre (voir tests/).
    """

    nb_customers: int = _int("NB_CUSTOMERS", 5000)
    nb_products: int = _int("NB_PRODUCTS", 800)
    nb_orders: int = _int("NB_ORDERS", 60000)
    seed: int = _int("SEED", 42)
    history_months: int = _int("HISTORY_MONTHS", 24)


ORACLE = OracleConfig()
CASSANDRA = CassandraConfig()
ELASTIC = ElasticConfig()
DATASET = DatasetConfig()


def ensure_dirs() -> None:
    """Cree l'arborescence des artefacts intermediaires (data/ est ignore par git)."""
    for directory in (JSON_DIR, PARQUET_DIR, REPORT_DIR):
        directory.mkdir(parents=True, exist_ok=True)
