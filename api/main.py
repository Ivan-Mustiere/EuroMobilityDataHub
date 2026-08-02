"""API REST EuroMobilityDataHub — sert les données réelles produites par le pipeline.

Lecture seule sur la base DuckDB de l'environnement APP_ENV (dev|preprod|prod).
Lancer : uvicorn api.main:app --reload --no-access-log
(--no-access-log : les access logs bruts d'uvicorn contiennent l'IP en clair, remplacés par notre
propre log anonymisé ci-dessous — cf. politique RGPD décrite dans le Bloc 1, partie 5.1/5.2/d.)
"""

import logging
import os
import pathlib
import time
from typing import Optional

import duckdb
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

load_dotenv()

ROOT = pathlib.Path(__file__).resolve().parent.parent
APP_ENV = os.getenv("APP_ENV", "dev")
DB_PATHS = {
    "dev": ROOT / "environments" / "dev" / "db_dev.duckdb",
    "preprod": ROOT / "environments" / "preprod" / "db_preprod.duckdb",
    "prod": ROOT / "environments" / "prod" / "db_prod.duckdb",
}

STATION_COLUMNS = ["station_id", "nom_gare", "trigramme", "code_uic", "latitude", "longitude"]
REGULARITE_COLUMNS = [
    "mois", "type_ligne", "axe_label", "nb_trains_prevus", "nb_trains_circules",
    "nb_trains_annules", "nb_trains_retard_arrivee", "retard_moyen_tous_trains_arrivee_min",
    "taux_ponctualite", "taux_annulation",
]

access_logger = logging.getLogger("euromobilitydatahub.access")
access_logger.setLevel(logging.INFO)
if not access_logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    access_logger.addHandler(_handler)

TAGS_METADATA = [
    {
        "name": "santé",
        "description": "Vérification de l'état du service et métriques d'exploitation (supervision).",
    },
    {
        "name": "gares",
        "description": "Référentiel des gares SNCF (nom, code, coordonnées GPS). Sert notamment au "
        "calcul de distance entre liaisons (formule de Haversine, cf. Bloc 2 partie 6.2).",
    },
    {
        "name": "régularité",
        "description": "Ponctualité, annulations et retards mensuels par liaison/région et par type "
        "de ligne (TER, Grande Vitesse, Intercités), à la base du baromètre (cf. Bloc 2 partie 3).",
    },
]

app = FastAPI(
    title="EuroMobilityDataHub API",
    description=(
        "API en lecture seule exposant les données réelles du baromètre ferroviaire "
        "(ponctualité, annulations, référentiel des gares) produites par le pipeline "
        "EuroMobilityDataHub à partir de données ouvertes SNCF (licence ODbL).\n\n"
        "Cette documentation interactive (Swagger) s'adresse aux profils techniques : elle "
        "permet de tester chaque endpoint directement depuis le navigateur. Pour une prise en "
        "main non technique, voir le guide `docs/Guide_prise_en_main_OEMF.md`."
    ),
    version="0.1.0",
    openapi_tags=TAGS_METADATA,
)

REQUEST_COUNT = Counter(
    "api_requests_total", "Nombre total de requêtes reçues par l'API", ["method", "path", "status"]
)
REQUEST_LATENCY = Histogram(
    "api_request_duration_seconds", "Durée des requêtes API, en secondes", ["method", "path"]
)


def anonymize_ip(ip: str) -> str:
    """Masque la partie identifiante d'une IP avant tout écriture en log.

    Méthode recommandée par la CNIL et décrite dans le Bloc 1 (partie 5.1/d) : les deux derniers
    octets d'une IPv4 sont masqués (ex. 82.45.12.7 -> 82.45.0.0), rendant la réidentification d'un
    utilisateur impossible tout en conservant assez d'information pour détecter des abus par plage
    géographique large. Pour IPv6, les 80 derniers bits (5 derniers groupes) sont masqués de la
    même manière.
    """
    if ip is None:
        return "unknown"
    if ":" in ip:
        groups = ip.split(":")
        return ":".join(groups[:3] + ["0"] * max(len(groups) - 3, 0))
    parts = ip.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.0.0"
    return "unknown"


@app.middleware("http")
async def anonymized_access_log_and_metrics(request: Request, call_next):
    """Journalise chaque requête avec une IP anonymisée (minimisation RGPD, cf. anonymize_ip) et
    enregistre les métriques Prometheus (nombre de requêtes, latence) exposées sur /metrics.
    """
    start = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start

    # Après le routing, request.scope contient la route résolue : on utilise son gabarit
    # ("/stations/{station_id}") plutôt que le chemin brut, pour éviter l'explosion de cardinalité
    # des métriques Prometheus si des identifiants uniques apparaissent dans l'URL.
    route = request.scope.get("route")
    path_label = route.path if route is not None else request.url.path

    REQUEST_COUNT.labels(method=request.method, path=path_label, status=response.status_code).inc()
    REQUEST_LATENCY.labels(method=request.method, path=path_label).observe(duration)

    client_ip = anonymize_ip(request.client.host if request.client else None)
    access_logger.info('%s - "%s %s" %s', client_ip, request.method, request.url.path, response.status_code)
    return response


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def get_connection() -> duckdb.DuckDBPyConnection:
    db_path = DB_PATHS.get(APP_ENV)
    if db_path is None or not db_path.exists():
        raise HTTPException(status_code=503, detail=f"Base introuvable pour l'environnement '{APP_ENV}' ({db_path})")
    return duckdb.connect(str(db_path), read_only=True)


def rows_to_dicts(rows: list, columns: list[str]) -> list[dict]:
    return [dict(zip(columns, row)) for row in rows]


@app.get(
    "/health",
    tags=["santé"],
    summary="Vérifier que l'API et sa base sont accessibles",
    response_description="Statut du service et environnement actif (dev, preprod ou prod)",
)
def health():
    return {"status": "ok", "env": APP_ENV}


@app.get(
    "/stations",
    tags=["gares"],
    summary="Lister les gares du référentiel",
    response_description="Liste des gares correspondant au filtre, avec coordonnées GPS",
)
def list_stations(
    q: Optional[str] = Query(None, description="Filtre sur le nom de gare (recherche partielle)"),
    limit: int = Query(50, le=500),
):
    con = get_connection()
    try:
        if q:
            rows = con.execute(
                f"SELECT {', '.join(STATION_COLUMNS)} FROM dim_stations "
                "WHERE nom_gare_norm LIKE ? ORDER BY nom_gare LIMIT ?",
                [f"%{q.upper()}%", limit],
            ).fetchall()
        else:
            rows = con.execute(
                f"SELECT {', '.join(STATION_COLUMNS)} FROM dim_stations ORDER BY nom_gare LIMIT ?",
                [limit],
            ).fetchall()
    finally:
        con.close()
    return rows_to_dicts(rows, STATION_COLUMNS)


@app.get(
    "/stations/{station_id}",
    tags=["gares"],
    summary="Récupérer une gare par son identifiant",
    response_description="Détail de la gare (nom, trigramme, code UIC, coordonnées GPS)",
)
def get_station(station_id: str):
    con = get_connection()
    try:
        row = con.execute(
            f"SELECT {', '.join(STATION_COLUMNS)} FROM dim_stations WHERE station_id = ?",
            [station_id],
        ).fetchone()
    finally:
        con.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Gare introuvable")
    return dict(zip(STATION_COLUMNS, row))


@app.get(
    "/regularite",
    tags=["régularité"],
    summary="Lister les indicateurs mensuels de régularité",
    response_description="Lignes de régularité (ponctualité, annulations, retard moyen) filtrées",
)
def list_regularite(
    type_ligne: Optional[str] = Query(None, description="grande_vitesse | regional | intercite"),
    mois: Optional[str] = Query(None, description="YYYY-MM"),
    axe_label: Optional[str] = Query(None, description="Filtre partiel sur le libellé de l'axe (liaison ou région)"),
    limit: int = Query(100, le=1000),
):
    con = get_connection()
    try:
        conditions, params = [], []
        if type_ligne:
            conditions.append("type_ligne = ?")
            params.append(type_ligne)
        if mois:
            conditions.append("mois = ?")
            params.append(mois)
        if axe_label:
            conditions.append("axe_label ILIKE ?")
            params.append(f"%{axe_label}%")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        rows = con.execute(
            f"""
            SELECT {', '.join(REGULARITE_COLUMNS)}
            FROM fact_regularite
            {where}
            ORDER BY mois DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    finally:
        con.close()
    return rows_to_dicts(rows, REGULARITE_COLUMNS)


@app.get(
    "/regularite/stats",
    tags=["régularité"],
    summary="Moyennes de ponctualité, annulation et retard par type de ligne",
    response_description="Une ligne par type de ligne (TER, Grande Vitesse, Intercités) avec ses moyennes",
)
def regularite_stats():
    con = get_connection()
    try:
        rows = con.execute(
            """
            SELECT type_ligne,
                   ROUND(AVG(taux_ponctualite), 2) AS taux_ponctualite_moyen,
                   ROUND(AVG(taux_annulation), 2) AS taux_annulation_moyen,
                   ROUND(AVG(retard_moyen_tous_trains_arrivee_min), 2) AS retard_moyen_min
            FROM fact_regularite
            GROUP BY type_ligne
            ORDER BY taux_ponctualite_moyen DESC
            """
        ).fetchall()
    finally:
        con.close()
    columns = ["type_ligne", "taux_ponctualite_moyen", "taux_annulation_moyen", "retard_moyen_min"]
    return rows_to_dicts(rows, columns)
