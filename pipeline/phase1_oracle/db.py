"""Acces a Oracle et execution des fichiers SQL du projet.

Le pilote python-oracledb est utilise en mode "thin" : il parle directement le
protocole reseau d'Oracle, sans Instant Client a installer. C'est ce qui permet
au projet de demarrer dans un Codespace vierge avec un simple `pip install`.

Convention de decoupage des fichiers SQL : une instruction se termine par un
"/" seul sur sa ligne, comme dans SQL*Plus. Ce separateur explicite evite
d'avoir a deviner si un ";" appartient a l'instruction ou a un bloc PL/SQL.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import oracledb

from pipeline.config import ORACLE

# Les CLOB sont renvoyes directement en str : sans cela, chaque document JSON
# extrait donnerait lieu a un aller-retour reseau supplementaire pour lire le
# LOB, ce qui domine largement le temps d'extraction.
oracledb.defaults.fetch_lobs = False

_MARKER = re.compile(r"^\s*--\s*@(\w+)\s*:\s*(.*)$")


def connect() -> oracledb.Connection:
    """Ouvre une connexion a la base applicative."""
    return oracledb.connect(user=ORACLE.user, password=ORACLE.password, dsn=ORACLE.dsn)


def _is_executable(statement: str) -> bool:
    """Vrai si le bloc contient autre chose que des commentaires."""
    for line in statement.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("--"):
            return True
    return False


def split_statements(sql_text: str) -> list[str]:
    """Decoupe un fichier SQL sur les lignes ne contenant qu'un "/"."""
    statements: list[str] = []
    buffer: list[str] = []
    for line in sql_text.splitlines():
        if line.strip() == "/":
            block = "\n".join(buffer).strip()
            if _is_executable(block):
                statements.append(block)
            buffer = []
        else:
            buffer.append(line)
    trailing = "\n".join(buffer).strip()
    if _is_executable(trailing):
        statements.append(trailing)
    return statements


@dataclass(frozen=True)
class NamedStatement:
    """Instruction SQL annotee par des marqueurs "-- @cle: valeur"."""

    name: str
    description: str
    severity: str
    sql: str


def _strip_leading_comments(lines: list[str]) -> str:
    """Retire l'en-tete de commentaires precedant l'instruction.

    Le bloc d'explication place en tete de fichier se retrouve sinon colle a la
    premiere instruction, ce qui brouille les journaux. Les commentaires
    internes a l'instruction, eux, sont conserves.
    """
    start = 0
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped and not stripped.startswith("--"):
            start = index
            break
    return "\n".join(lines[start:]).strip()


def parse_named_statements(path: Path) -> list[NamedStatement]:
    """Lit un fichier SQL dont chaque instruction porte au moins un marqueur @name."""
    statements = []
    for block in split_statements(path.read_text(encoding="utf-8")):
        meta: dict[str, str] = {}
        body: list[str] = []
        for line in block.splitlines():
            match = _MARKER.match(line)
            if match:
                meta[match.group(1)] = match.group(2).strip()
            else:
                body.append(line)
        name = meta.get("name")
        if not name:
            continue
        statements.append(
            NamedStatement(
                name=name,
                description=meta.get("desc", ""),
                severity=meta.get("severity", "blocking"),
                sql=_strip_leading_comments(body),
            )
        )
    return statements


def run_script(connection: oracledb.Connection, path: Path) -> int:
    """Execute toutes les instructions d'un fichier SQL et renvoie leur nombre."""
    statements = split_statements(path.read_text(encoding="utf-8"))
    with connection.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
    connection.commit()
    return len(statements)


def read_query(path: Path) -> str:
    """Lit un fichier ne contenant qu'une seule requete, sans son en-tete de commentaires."""
    lines = path.read_text(encoding="utf-8").splitlines()
    return _strip_leading_comments(lines).rstrip(";")


def iter_query(connection: oracledb.Connection, sql: str, arraysize: int = 500) -> Iterator[tuple]:
    """Parcourt une requete en flux, sans charger le resultat complet en memoire.

    `arraysize` fixe le nombre de lignes rapatriees par aller-retour reseau.
    Les documents JSON pesant quelques kilo-octets, 500 lignes par lot est un
    bon compromis entre le nombre d'allers-retours et l'empreinte memoire.
    """
    with connection.cursor() as cursor:
        cursor.arraysize = arraysize
        cursor.prefetchrows = arraysize + 1
        cursor.execute(sql)
        while True:
            rows = cursor.fetchmany(arraysize)
            if not rows:
                return
            yield from rows


def table_counts(connection: oracledb.Connection) -> dict[str, int]:
    """Compte les lignes de chaque table applicative (controle de bout en bout)."""
    tables = [
        "countries",
        "payment_methods",
        "categories",
        "products",
        "customers",
        "addresses",
        "orders",
        "order_items",
    ]
    counts = {}
    with connection.cursor() as cursor:
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
    return counts
