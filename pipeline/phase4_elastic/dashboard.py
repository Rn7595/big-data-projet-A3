"""Creation des vues de donnees Kibana et du tableau de bord.

Le tableau de bord est genere par code plutot que construit a la souris. La
raison est la reproductibilite : un dashboard dessine dans l'interface vit dans
la base interne de Kibana et disparait avec le conteneur. Ici, il est decrit
dans le depot, versionne, et reconstruit a l'identique par une commande.

Trois sous-commandes :

    python -m pipeline.phase4_elastic.dashboard build    vues + import du dashboard
    python -m pipeline.phase4_elastic.dashboard export   sauvegarde ce que contient Kibana
    python -m pipeline.phase4_elastic.dashboard views    vues de donnees seules

`export` sert de filet : si le dashboard est retouche dans l'interface, cette
commande le renvoie dans le depot pour qu'il ne soit pas perdu.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import requests

from pipeline.config import ELASTIC, ROOT
from pipeline.utils import get_logger, step

LOGGER = get_logger("phase4.dashboard")

KIBANA_DIR = ROOT / "kibana"
NDJSON_FILE = KIBANA_DIR / "dashboard.ndjson"
HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

DASHBOARD_ID = "ecommerce-dashboard"
DASHBOARD_TITLE = "E-commerce -- ventes, catalogue et clients"


# --------------------------------------------------------------------- Kibana

def wait_for_kibana(timeout: int = 300) -> None:
    """Attend que Kibana reponde l'etat `available`.

    Kibana demarre plusieurs minutes apres Elasticsearch : il migre ses index
    internes avant d'accepter la moindre requete d'API.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status = requests.get(f"{ELASTIC.kibana_url}/api/status", timeout=10).json()
            if status.get("status", {}).get("overall", {}).get("level") == "available":
                LOGGER.info("Kibana est disponible (version %s)",
                            status.get("version", {}).get("number", "?"))
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise RuntimeError(f"Kibana n'a pas repondu 'available' en {timeout} s")


def create_data_view(identifier: str, title: str, name: str, time_field: str | None) -> None:
    """Declare une vue de donnees, c'est-a-dire ce que Kibana sait interroger.

    L'identifiant est impose plutot que genere : les panneaux du tableau de bord
    y font reference, et une reconstruction doit retomber sur les memes liens.
    """
    payload: dict = {"data_view": {"id": identifier, "title": title, "name": name},
                     "override": True}
    if time_field:
        payload["data_view"]["timeFieldName"] = time_field

    response = requests.post(f"{ELASTIC.kibana_url}/api/data_views/data_view",
                             headers=HEADERS, json=payload, timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(f"Creation de la vue {title} refusee : {response.text[:400]}")
    LOGGER.info("Vue de donnees %-22s -> %s", identifier, title)


def create_data_views() -> None:
    create_data_view("ecom-order-items", ELASTIC.index_items,
                     "Lignes de commande", "order_date")
    # Pas de champ temporel sur les clients : la segmentation est un etat a
    # l'instant du calcul, pas une serie temporelle. Lui en donner un
    # soumettrait le panneau au selecteur de periode du tableau de bord et le
    # viderait des que l'utilisateur restreint la fenetre.
    create_data_view("ecom-customers", ELASTIC.index_customers, "Clients (RFM)", None)


# ------------------------------------------------------------- objets sauvegardes

def _sum_column(field: str, label: str) -> dict:
    return {
        "label": label, "dataType": "number", "operationType": "sum",
        "sourceField": field, "isBucketed": False, "scale": "ratio",
        "params": {"emptyAsNull": False},
    }


def _count_column(label: str = "Nombre de lignes") -> dict:
    return {
        "label": label, "dataType": "number", "operationType": "count",
        "sourceField": "___records___", "isBucketed": False, "scale": "ratio",
        "params": {"emptyAsNull": False},
    }


def _unique_column(field: str, label: str) -> dict:
    return {
        "label": label, "dataType": "number", "operationType": "unique_count",
        "sourceField": field, "isBucketed": False, "scale": "ratio",
        "params": {"emptyAsNull": False},
    }


def _terms_column(field: str, label: str, order_by: str, size: int = 10) -> dict:
    return {
        "label": label, "dataType": "string", "operationType": "terms",
        "sourceField": field, "isBucketed": True, "scale": "ordinal",
        "params": {
            "size": size,
            "orderBy": {"type": "column", "columnId": order_by},
            "orderDirection": "desc",
            "otherBucket": False, "missingBucket": False,
            "parentFormat": {"id": "terms"},
        },
    }


def _date_column(field: str, interval: str = "1M") -> dict:
    return {
        "label": field, "dataType": "date", "operationType": "date_histogram",
        "sourceField": field, "isBucketed": True, "scale": "interval",
        "params": {"interval": interval, "includeEmptyRows": True, "dropPartials": False},
    }


def _lens(identifier: str, title: str, visualization_type: str, columns: dict,
          column_order: list[str], visualization: dict, data_view: str,
          query: str = "") -> dict:
    return {
        "id": identifier,
        "type": "lens",
        "attributes": {
            "title": title,
            "visualizationType": visualization_type,
            "state": {
                "datasourceStates": {
                    "formBased": {
                        "layers": {
                            "couche": {
                                "columns": columns,
                                "columnOrder": column_order,
                                "incompleteColumns": {},
                                "sampling": 1,
                            }
                        }
                    }
                },
                "filters": [],
                "query": {"language": "kuery", "query": query},
                "visualization": visualization,
            },
        },
        "references": [{
            "type": "index-pattern",
            "id": data_view,
            "name": "indexpattern-datasource-layer-couche",
        }],
    }


def _xy(series_type: str, x: str, accessors: list[str]) -> dict:
    return {
        "legend": {"isVisible": True, "position": "right"},
        "valueLabels": "hide",
        "preferredSeriesType": series_type,
        "layers": [{
            "layerId": "couche", "accessors": accessors, "position": "top",
            "seriesType": series_type, "showGridlines": False,
            "layerType": "data", "xAccessor": x,
        }],
    }


def build_visualisations() -> list[dict]:
    """Les huit panneaux du tableau de bord.

    Ils repondent aux questions qu'un responsable e-commerce se pose : combien
    ai-je vendu, quand, de quoi, ou, et a qui.
    """
    items = "ecom-order-items"
    clients = "ecom-customers"
    revenu = "is_revenue: true"

    return [
        # net_amount vaut deja zero pour les commandes annulees : la somme est
        # juste sans qu'aucun filtre ne soit necessaire.
        _lens("viz-ca-total", "Chiffre d'affaires net", "lnsMetric",
              {"mesure": _sum_column("net_amount", "Chiffre d'affaires net")},
              ["mesure"],
              {"layerId": "couche", "layerType": "data", "metricAccessor": "mesure"},
              items),

        _lens("viz-nb-commandes", "Commandes facturees", "lnsMetric",
              {"mesure": _unique_column("order_id", "Commandes facturees")},
              ["mesure"],
              {"layerId": "couche", "layerType": "data", "metricAccessor": "mesure"},
              items, query=revenu),

        _lens("viz-ca-mois", "Chiffre d'affaires par mois", "lnsXY",
              {"periode": _date_column("order_date", "1M"),
               "mesure": _sum_column("net_amount", "Chiffre d'affaires")},
              ["periode", "mesure"],
              _xy("bar_stacked", "periode", ["mesure"]),
              items),

        _lens("viz-ca-rayon", "Repartition par rayon", "lnsPie",
              {"rayon": _terms_column("parent_category_name", "Rayon", "mesure", 8),
               "mesure": _sum_column("net_amount", "Chiffre d'affaires")},
              ["rayon", "mesure"],
              {"shape": "donut", "layers": [{
                  "layerId": "couche", "primaryGroups": ["rayon"], "metrics": ["mesure"],
                  "numberDisplay": "percent", "categoryDisplay": "default",
                  "legendDisplay": "default", "nestedLegend": False, "layerType": "data",
              }]},
              items),

        _lens("viz-top-produits", "Meilleures ventes", "lnsDatatable",
              {"produit": _terms_column("product_name.keyword", "Produit", "mesure", 15),
               "mesure": _sum_column("net_amount", "Chiffre d'affaires"),
               "quantite": _sum_column("quantity", "Quantite vendue")},
              ["produit", "mesure", "quantite"],
              {"layerId": "couche", "layerType": "data", "columns": [
                  {"columnId": "produit", "isTransposed": False},
                  {"columnId": "mesure", "isTransposed": False},
                  {"columnId": "quantite", "isTransposed": False},
              ]},
              items, query=revenu),

        _lens("viz-ca-pays", "Chiffre d'affaires par pays", "lnsXY",
              {"pays": _terms_column("country_name", "Pays", "mesure", 8),
               "mesure": _sum_column("net_amount", "Chiffre d'affaires")},
              ["pays", "mesure"],
              _xy("bar_horizontal", "pays", ["mesure"]),
              items),

        _lens("viz-statuts", "Repartition des statuts de commande", "lnsXY",
              {"statut": _terms_column("order_status", "Statut", "mesure", 6),
               "mesure": _count_column("Lignes de commande")},
              ["statut", "mesure"],
              _xy("bar_stacked", "statut", ["mesure"]),
              items),

        _lens("viz-segments", "Segmentation RFM des clients", "lnsXY",
              {"segment": _terms_column("segment", "Segment", "mesure", 8),
               "mesure": _count_column("Clients")},
              ["segment", "mesure"],
              _xy("bar_horizontal", "segment", ["mesure"]),
              clients),
    ]


# Disposition sur la grille Kibana, large de 48 colonnes.
LAYOUT = [
    ("viz-ca-total", 0, 0, 12, 8),
    ("viz-nb-commandes", 12, 0, 12, 8),
    ("viz-ca-rayon", 24, 0, 24, 16),
    ("viz-ca-mois", 0, 8, 24, 16),
    ("viz-top-produits", 0, 24, 24, 16),
    ("viz-ca-pays", 24, 16, 24, 12),
    ("viz-statuts", 24, 28, 24, 12),
    ("viz-segments", 0, 40, 48, 12),
]


def build_dashboard(version: str = "8.15.3") -> dict:
    panels, references = [], []
    for position, (viz_id, x, y, width, height) in enumerate(LAYOUT, start=1):
        panels.append({
            "version": version, "type": "lens",
            "gridData": {"x": x, "y": y, "w": width, "h": height, "i": str(position)},
            "panelIndex": str(position), "embeddableConfig": {},
            "panelRefName": f"panel_{position}",
        })
        references.append({"name": f"panel_{position}", "type": "lens", "id": viz_id})

    return {
        "id": DASHBOARD_ID,
        "type": "dashboard",
        "attributes": {
            "title": DASHBOARD_TITLE,
            "description": "Genere par pipeline.phase4_elastic.dashboard",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({"useMargins": True, "syncColors": False,
                                       "syncCursor": True, "syncTooltips": False,
                                       "hidePanelTitles": False}),
            # La periode est enregistree avec le tableau de bord : a l'ouverture
            # il couvre l'historique complet, sans reglage manuel.
            "timeRestore": True,
            "timeFrom": "now-25M",
            "timeTo": "now",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(
                    {"query": {"query": "", "language": "kuery"}, "filter": []})
            },
        },
        "references": references,
    }


def generate_ndjson() -> str:
    objects = build_visualisations() + [build_dashboard()]
    lines = [json.dumps(obj, ensure_ascii=False) for obj in objects]
    lines.append(json.dumps({"excludedObjects": [], "excludedObjectsCount": 0,
                             "exportedCount": len(objects), "missingRefCount": 0,
                             "missingReferences": []}))
    return "\n".join(lines) + "\n"


def import_objects(content: str) -> None:
    response = requests.post(
        f"{ELASTIC.kibana_url}/api/saved_objects/_import",
        params={"overwrite": "true"},
        headers={"kbn-xsrf": "true"},
        files={"file": ("dashboard.ndjson", content, "application/ndjson")},
        timeout=120,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Import refuse par Kibana : {response.text[:600]}")

    result = response.json()
    if not result.get("success"):
        errors = json.dumps(result.get("errors", []), ensure_ascii=False)[:800]
        raise RuntimeError(f"Import partiel : {errors}")
    LOGGER.info("%d objets importes dans Kibana", result.get("successCount", 0))


def export_objects() -> None:
    """Renvoie dans le depot ce que contient Kibana."""
    response = requests.post(
        f"{ELASTIC.kibana_url}/api/saved_objects/_export",
        headers=HEADERS,
        json={"type": ["dashboard", "lens", "index-pattern"], "includeReferencesDeep": True},
        timeout=120,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Export refuse : {response.text[:400]}")
    KIBANA_DIR.mkdir(parents=True, exist_ok=True)
    NDJSON_FILE.write_text(response.text, encoding="utf-8")
    LOGGER.info("Objets Kibana exportes dans %s (%d lignes)",
                NDJSON_FILE, response.text.count("\n"))


def main() -> None:
    commande = sys.argv[1] if len(sys.argv) > 1 else "build"
    wait_for_kibana()

    if commande == "views":
        create_data_views()
        return

    if commande == "export":
        export_objects()
        return

    with step(LOGGER, "creation des vues de donnees"):
        create_data_views()

    KIBANA_DIR.mkdir(parents=True, exist_ok=True)
    content = generate_ndjson()
    NDJSON_FILE.write_text(content, encoding="utf-8")

    with step(LOGGER, "import du tableau de bord"):
        try:
            import_objects(content)
        except RuntimeError as error:
            # L'indexation, elle, a reussi : mieux vaut expliquer comment
            # terminer a la main que faire echouer toute la phase.
            LOGGER.error("%s", error)
            LOGGER.error("Les vues de donnees sont en place : le tableau de bord peut etre")
            LOGGER.error("construit dans Kibana, puis renvoye dans le depot par :")
            LOGGER.error("    python -m pipeline.phase4_elastic.dashboard export")
            raise SystemExit(1)

    LOGGER.info("Tableau de bord disponible : %s/app/dashboards#/view/%s",
                ELASTIC.kibana_url, DASHBOARD_ID)


if __name__ == "__main__":
    main()
