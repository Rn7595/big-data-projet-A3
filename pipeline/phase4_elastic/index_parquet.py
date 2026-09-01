"""Indexation des fichiers Parquet dans Elasticsearch.

Deux index sont crees :

  * `ecom-order-items`, au grain de la ligne de commande. C'est le grain le
    plus fin disponible : Kibana peut en deduire n'importe quel agregat, la ou
    un index deja agrege ne repondrait qu'aux questions prevues d'avance ;
  * `ecom-customers`, au grain du client, portant la segmentation RFM.

Les agregats calcules en phase 3 ne sont volontairement pas indexes : ils
feraient double emploi avec ce qu'Elasticsearch sait calculer lui-meme, et
introduiraient un risque d'incoherence entre deux sources du meme chiffre. Ils
restent en Parquet, ou ils servent de reference pour verifier les totaux
affiches par Kibana.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.dataset as ds
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

from pipeline.config import ELASTIC, PARQUET_DIR, REPORT_DIR, ensure_dirs
from pipeline.utils import get_logger, step

LOGGER = get_logger("phase4.indexation")

MAPPINGS_DIR = Path(__file__).parent / "mappings"

# Taille des lots envoyes a l'API _bulk. Trop petit, on multiplie les
# allers-retours ; trop gros, on depasse la memoire allouee aux requetes cote
# serveur. Quelques milliers de documents par lot est le compromis usuel.
BULK_SIZE = 2_000
ARROW_BATCH = 10_000


def connect() -> Elasticsearch:
    client = Elasticsearch(ELASTIC.url, request_timeout=120)
    info = client.info()
    LOGGER.info("Connecte a Elasticsearch %s (cluster %s)",
                info["version"]["number"], info["cluster_name"])
    return client


def create_index(client: Elasticsearch, index: str, mapping_file: str) -> None:
    """Recree un index a partir de son mapping explicite.

    Le mapping est declare, jamais devine. En mapping dynamique,
    Elasticsearch aurait typé les identifiants numeriques en `long` et les
    libelles en `text`, ce qui les rendrait inutilisables comme critere de
    regroupement dans Kibana. `dynamic: strict` fait echouer l'indexation d'un
    champ non declare, plutot que de l'accepter silencieusement : une colonne
    ajoutee en amont sans mise a jour du mapping se voit immediatement.
    """
    body = json.loads((MAPPINGS_DIR / mapping_file).read_text(encoding="utf-8"))
    if client.indices.exists(index=index):
        client.indices.delete(index=index)
    client.indices.create(index=index, **body)
    nb_champs = len(body["mappings"]["properties"])
    LOGGER.info("Index %s cree (%d champs declares)", index, nb_champs)


def read_parquet_documents(path: Path) -> Iterator[dict]:
    """Lit un jeu Parquet par lots et le convertit en documents JSON.

    La lecture se fait par lots plutot qu'en un bloc : la table de faits
    represente pres de 150 000 lignes, dont la materialisation complete en
    dictionnaires Python couterait plusieurs centaines de mega-octets.

    Deux conversions sont necessaires. Les colonnes monetaires sont stockees en
    `decimal` dans Parquet, type que le JSON ne connait pas : elles sont
    converties en flottant, la precision etant ensuite garantie cote
    Elasticsearch par le type `scaled_float`. Les colonnes de partitionnement
    reviennent quant a elles sous forme de dictionnaires encodes, qu'il faut
    ramener a leur type d'origine.
    """
    dataset = ds.dataset(str(path), format="parquet", partitioning="hive")

    for batch in dataset.to_batches(batch_size=ARROW_BATCH):
        arrays, names = [], []
        for index, field in enumerate(batch.schema):
            column = batch.column(index)
            if pa.types.is_decimal(field.type):
                column = column.cast(pa.float64())
            elif pa.types.is_dictionary(field.type):
                column = column.dictionary_decode()
                if pa.types.is_string(column.type):
                    # Les valeurs de partition sont lues comme du texte ;
                    # order_year et order_month doivent redevenir numeriques.
                    try:
                        column = column.cast(pa.int32())
                    except pa.ArrowInvalid:
                        pass
            arrays.append(column)
            names.append(field.name)

        for document in pa.RecordBatch.from_arrays(arrays, names=names).to_pylist():
            yield {key: value for key, value in document.items() if value is not None}


def index_documents(client: Elasticsearch, index: str, source: Path, id_fields: list[str]) -> int:
    """Indexe un jeu Parquet et renvoie le nombre de documents ecrits.

    L'identifiant du document est derive de ses cles metier plutot que laisse a
    Elasticsearch. Consequence : reindexer met a jour les documents existants au
    lieu d'en creer des doublons. L'operation devient rejouable, ce qui compte
    quand on relance une demonstration.
    """

    def actions() -> Iterator[dict]:
        for document in read_parquet_documents(source):
            yield {
                "_index": index,
                "_id": "-".join(str(document[field]) for field in id_fields),
                "_source": document,
            }

    written = 0
    for ok, item in streaming_bulk(client, actions(), chunk_size=BULK_SIZE,
                                   raise_on_error=True, max_retries=3):
        written += 1
        if written % 25_000 == 0:
            LOGGER.info("    %d documents indexes", written)
    return written


def main() -> None:
    ensure_dirs()
    facts_dir = PARQUET_DIR / "fact_order_items"
    if not facts_dir.exists():
        raise RuntimeError(f"{facts_dir} est absent : la phase 3 doit etre executee avant.")

    client = connect()
    report: dict[str, int] = {}

    with step(LOGGER, "creation des index"):
        create_index(client, ELASTIC.index_items, "order_items.json")
        create_index(client, ELASTIC.index_customers, "customers.json")

    with step(LOGGER, f"indexation de {ELASTIC.index_items}"):
        report[ELASTIC.index_items] = index_documents(
            client, ELASTIC.index_items, facts_dir, ["order_id", "line_no"])

    with step(LOGGER, f"indexation de {ELASTIC.index_customers}"):
        report[ELASTIC.index_customers] = index_documents(
            client, ELASTIC.index_customers, PARQUET_DIR / "dim_customers_rfm", ["customer_id"])

    # `refresh` force la mise a disposition des documents pour la recherche :
    # sans lui, les comptages qui suivent porteraient sur un index encore
    # partiellement invisible, l'indexation etant asynchrone par defaut.
    for index in (ELASTIC.index_items, ELASTIC.index_customers):
        client.indices.refresh(index=index)
        count = client.count(index=index)["count"]
        LOGGER.info("  %-24s %8d documents indexes, %8d documents dans l'index",
                    index, report[index], count)
        if count != report[index]:
            raise RuntimeError(f"{index} : {report[index]} envoyes mais {count} presents")

    # Controle de bout en bout : le chiffre d'affaires calcule par
    # Elasticsearch doit egaler celui calcule par Spark.
    agg = client.search(index=ELASTIC.index_items, size=0, aggs={
        "ca": {"sum": {"field": "net_amount"}},
        "commandes": {"cardinality": {"field": "order_id", "precision_threshold": 40000}},
    })
    ca_total = agg["aggregations"]["ca"]["value"]
    LOGGER.info("Chiffre d'affaires indexe : %s EUR", f"{ca_total:,.2f}")
    LOGGER.info("A comparer avec agg_sales_by_month (phase 3) : les deux doivent coincider")
    report["chiffre_affaires"] = round(ca_total, 2)
    report["commandes_distinctes"] = agg["aggregations"]["commandes"]["value"]

    report_file = REPORT_DIR / "phase4_indexation.json"
    report_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    LOGGER.info("Rapport ecrit dans %s", report_file)


if __name__ == "__main__":
    main()
