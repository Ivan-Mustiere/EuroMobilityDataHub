"""API REST EuroMobilityDataHub — sert les données réelles produites par le pipeline.

Lecture seule sur la base DuckDB de l'environnement APP_ENV (dev|preprod|prod).
Lancer : uvicorn apps.api.main:app --reload --no-access-log
(--no-access-log : les access logs bruts d'uvicorn contiennent l'IP en clair, remplacés par notre
propre log anonymisé ci-dessous — cf. politique RGPD décrite dans le Bloc 1, partie 5.1/5.2/d.)
"""

import json
import logging
import os
import pathlib
import re
import time
from typing import Optional

import duckdb
import psycopg
import requests
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

load_dotenv()

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
APP_ENV = os.getenv("APP_ENV", "dev")
STATIC_DIR = pathlib.Path(__file__).resolve().parent / "static"

# Rôle api_consumer (Bloc 1, Tableau 13) : accès en lecture seule aux endpoints de données,
# identifié par une clé API, avec quotas de requêtes. Pas de clé configurée -> aucun accès
# (échec fermé), plutôt qu'une API ouverte par erreur de configuration en prod.
VALID_API_KEYS = {k.strip() for k in os.getenv("API_KEYS", "").split(",") if k.strip()}
# CARTO exige désormais une clé (gratuite) pour ses tuiles de fond de carte (basemaps.cartocdn.com),
# injectée dans radar.html à la volée pour permettre une clé différente par environnement.
CARTO_API_KEY = os.getenv("CARTO_API_KEY", "")

# API officielle SNCF (Navitia, https://numerique.sncf.com) : seule source qui donne un texte de
# cause ("Accident de personne", "Défaillance de matériel"...), le flux GTFS-RT (producer.py) ne
# publiant qu'un délai en secondes. Authentification Basic HTTP, la clé en identifiant, sans mot de
# passe. Optionnelle : sans clé, `cause_retard` reste toujours à `null` (pas d'erreur).
SNCF_API_KEY = os.getenv("SNCF_API_KEY", "")
_TRAIN_NUMBER_RE = re.compile(r"^OCESN(\d+)F")
_disruptions_cache = {"fetched_at": 0.0, "by_train_number": {}}
_DISRUPTIONS_CACHE_TTL_S = 90
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
    {
        "name": "temps réel",
        "description": "Position estimée des trains actuellement en circulation, à partir du flux "
        "GTFS-RT SNCF (retards par arrêt, rafraîchi ~toutes les 2 min). La SNCF ne publiant pas de "
        "position GPS par train, la position est interpolée entre les deux gares qui encadrent "
        "l'heure courante — voir le champ `statut` et la documentation de l'endpoint.",
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
        "main non technique, voir le guide `docs/Guide_prise_en_main_OEMF.md`.\n\n"
        "Accès aux données (`/stations*`, `/regularite*`, `/realtime/*`) : en-tête `X-API-Key` requis, "
        "30 requêtes/minute par clé (rôle api_consumer, Bloc 1 Tableau 13). "
        "`/health` et `/metrics` restent ouverts (supervision)."
    ),
    version="0.1.0",
    openapi_tags=TAGS_METADATA,
)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# API publique en lecture seule destinée à être appelée depuis des sites tiers (cf. objectif de
# l'endpoint /realtime/trains : permettre à n'importe qui de construire sa propre carte
# interactive) : CORS ouvert par conception, pas un oubli — l'accès est déjà contrôlé par la clé
# API et le quota, pas par l'origine de la requête.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["X-API-Key"],
)

REQUEST_COUNT = Counter(
    "api_requests_total", "Nombre total de requêtes reçues par l'API", ["method", "path", "status"]
)
REQUEST_LATENCY = Histogram(
    "api_request_duration_seconds", "Durée des requêtes API, en secondes", ["method", "path"]
)


def require_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")) -> None:
    if not x_api_key or x_api_key not in VALID_API_KEYS:
        raise HTTPException(
            status_code=401,
            detail="Clé API manquante ou invalide (en-tête X-API-Key requis, cf. rôle api_consumer)",
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


# --- Temps réel (fact_realtime, Postgres) --------------------------------------------------
#
# Même schéma d'identifiants que consumer.py (RDS_SECRET_NAME/AWS_REGION en prod, PG* en local) :
# l'API lit la même base que le consumer y écrit, cf. infra/cloud/docker-compose.yml.

_STOP_ID_UIC_RE = re.compile(r"(\d{7,8})$")


def _pg_credentials_from_secrets_manager() -> dict:
    import boto3

    client = boto3.client("secretsmanager", region_name=os.getenv("AWS_REGION"))
    secret = json.loads(client.get_secret_value(SecretId=os.environ["RDS_SECRET_NAME"])["SecretString"])
    return {
        "host": secret["host"],
        "port": secret.get("port", 5432),
        "dbname": secret["dbname"],
        "user": secret["username"],
        "password": secret["password"],
    }


def _pg_credentials_from_env() -> dict:
    return {
        "host": os.environ["PGHOST"],
        "port": os.environ.get("PGPORT", 5432),
        "dbname": os.environ["PGDATABASE"],
        "user": os.environ["PGUSER"],
        "password": os.environ["PGPASSWORD"],
    }


def get_pg_connection() -> psycopg.Connection:
    try:
        creds = _pg_credentials_from_secrets_manager() if os.getenv("RDS_SECRET_NAME") else _pg_credentials_from_env()
        return psycopg.connect(
            f"host={creds['host']} port={creds['port']} dbname={creds['dbname']} "
            f"user={creds['user']} password={creds['password']}",
            connect_timeout=5,
        )
    except (KeyError, psycopg.OperationalError) as exc:
        raise HTTPException(status_code=503, detail=f"Base temps réel indisponible : {exc!r}")


def _stations_by_uic() -> dict[str, dict]:
    """code_uic -> {nom_gare, latitude, longitude}, à partir du référentiel DuckDB (dim_stations).

    Le flux temps réel ne connaît que des stop_id GTFS-RT ; on en extrait le code UIC (7-8 chiffres
    en fin d'identifiant, ex. 'StopPoint:OCETGV INOUI-87688887' -> '87688887') pour le rapprocher du
    référentiel des gares — même logique de rapprochement partiel que dim_liaisons (Bloc 2 §6.2) :
    NULL/absent si non trouvé, jamais de position inventée.
    """
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT code_uic, nom_gare, latitude, longitude FROM dim_stations "
            "WHERE code_uic IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL"
        ).fetchall()
    finally:
        con.close()
    return {uic: {"nom_gare": nom, "latitude": lat, "longitude": lon} for uic, nom, lat, lon in rows}


def _uic_from_stop_id(stop_id: str) -> Optional[str]:
    match = _STOP_ID_UIC_RE.search(stop_id or "")
    return match.group(1) if match else None


def _train_number_from_trip_id(trip_id: str) -> Optional[str]:
    match = _TRAIN_NUMBER_RE.match(trip_id or "")
    return match.group(1) if match else None


def _fetch_active_disruptions() -> dict:
    """Cause en cours par numéro de train (ex. "9852"), via l'API SNCF officielle.

    `since`/`until` figés à l'instant présent : ne renvoie que les perturbations dont la période
    d'application couvre "maintenant", ce qui évite au passage de confondre deux circulations d'un
    même numéro de train à des dates différentes (les numéros sont réutilisés d'un jour à l'autre).
    """
    now_str = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    try:
        response = requests.get(
            "https://api.sncf.com/v1/coverage/sncf/disruptions",
            params={"count": 500, "since": now_str, "until": now_str},
            auth=(SNCF_API_KEY, ""),
            timeout=10,
        )
        response.raise_for_status()
        disruptions = response.json().get("disruptions", [])
    except Exception as exc:
        logging.warning("SNCF disruptions indisponible (%r), on garde le dernier résultat connu", exc)
        return _disruptions_cache["by_train_number"]

    by_train_number = {}
    for disruption in disruptions:
        messages = [m["text"] for m in disruption.get("messages", []) if m.get("text")]
        if not messages:
            continue
        for impacted in disruption.get("impacted_objects", []):
            train_number = impacted.get("pt_object", {}).get("trip", {}).get("name")
            if train_number:
                by_train_number[train_number] = {
                    "cause": messages[0],
                    "gravite": disruption.get("severity", {}).get("effect"),
                }
    return by_train_number


def _active_disruptions_cached() -> dict:
    if not SNCF_API_KEY:
        return {}
    if time.time() - _disruptions_cache["fetched_at"] > _DISRUPTIONS_CACHE_TTL_S:
        _disruptions_cache["by_train_number"] = _fetch_active_disruptions()
        _disruptions_cache["fetched_at"] = time.time()
    return _disruptions_cache["by_train_number"]


def _remaining_route_coords(stops: list[dict], index_restant: int, stations: dict) -> list[list[float]]:
    """Coordonnées des gares restantes du trajet à partir du segment courant (`index_restant`,
    cf. `_interpolate_trip`), pour tracer l'itinéraire d'un train sélectionné sur la carte. Ignore
    les arrêts non rapprochés du référentiel gares (pas de position inventée) et les doublons
    consécutifs (sous-quais, même logique que pour l'interpolation de position)."""
    usable = [s for s in stops if s.get("arrival_time") or s.get("departure_time")]
    route = []
    last_uic = None
    for s in usable[index_restant:]:
        uic = _uic_from_stop_id(s["stop_id"])
        gare = stations.get(uic)
        if gare is None or uic == last_uic:
            continue
        route.append([gare["latitude"], gare["longitude"]])
        last_uic = uic
    return route


def _interpolate_trip(stops: list[dict], now: int) -> Optional[dict]:
    """Position d'un train entre les deux arrêts qui encadrent l'heure `now` (epoch Unix).

    À partir de l'horaire prédit du flux GTFS-RT (arrival_time/departure_time par arrêt) — la SNCF
    ne publiant pas de position GPS par train (pas de flux GTFS-RT vehicle-positions), il s'agit
    d'une interpolation linéaire sur l'horaire, pas d'une position réelle. Retourne None si le
    trajet est terminé ou si l'horaire est insuffisant pour statuer (pas de valeur inventée).
    """
    usable = [s for s in stops if s.get("arrival_time") or s.get("departure_time")]
    if not usable:
        return None

    # Train à quai en cours de route (arrêt intermédiaire) : now tombe entre l'arrivée et le
    # départ prévus d'un même arrêt. Sans ce cas, le train disparaissait de la carte pendant tout
    # son temps d'arrêt en gare (aucun des segments ci-dessous ne le couvre).
    for index, s in enumerate(usable):
        arrival_time, departure_time = s.get("arrival_time"), s.get("departure_time")
        if arrival_time is not None and departure_time is not None and arrival_time <= now < departure_time:
            return {
                "statut": "a_quai",
                "stop_id_precedent": s["stop_id"],
                "stop_id_suivant": s["stop_id"],
                "progression": 0.0,
                "retard_s": s.get("departure_delay") or s.get("arrival_delay"),
                "index_restant": index,
            }

    if len(usable) < 2:
        return None

    for i in range(len(usable) - 1):
        dep = usable[i]["departure_time"] or usable[i]["arrival_time"]
        arr = usable[i + 1]["arrival_time"] or usable[i + 1]["departure_time"]
        if dep is None or arr is None:
            continue
        if dep <= now <= arr:
            fraction = 0.0 if arr <= dep else (now - dep) / (arr - dep)
            return {
                "statut": "en_route",
                "stop_id_precedent": usable[i]["stop_id"],
                "stop_id_suivant": usable[i + 1]["stop_id"],
                "progression": round(fraction, 3),
                "retard_s": usable[i + 1].get("arrival_delay") or usable[i + 1].get("departure_delay"),
                "index_restant": i,
            }

    first_time = usable[0]["departure_time"] or usable[0]["arrival_time"]
    if first_time is not None and now < first_time:
        return {
            "statut": "a_quai",
            "stop_id_precedent": usable[0]["stop_id"],
            "stop_id_suivant": usable[0]["stop_id"],
            "progression": 0.0,
            "retard_s": usable[0].get("departure_delay") or usable[0].get("arrival_delay"),
            "index_restant": 0,
        }

    return None  # trajet terminé (dernier arrêt déjà atteint) : on ne l'affiche plus


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
    dependencies=[Depends(require_api_key)],
)
@limiter.limit("30/minute")
def list_stations(
    request: Request,
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
    dependencies=[Depends(require_api_key)],
)
@limiter.limit("30/minute")
def get_station(request: Request, station_id: str):
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
    dependencies=[Depends(require_api_key)],
)
@limiter.limit("30/minute")
def list_regularite(
    request: Request,
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
    dependencies=[Depends(require_api_key)],
)
@limiter.limit("30/minute")
def regularite_stats(request: Request):
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


@app.get(
    "/realtime/map",
    tags=["temps réel"],
    summary="Démo : carte interactive des trains en circulation (page HTML)",
    response_description="Page HTML statique qui sonde /realtime/trains toutes les 15s.",
    include_in_schema=False,
)
def realtime_map():
    # Servie en même origine que /realtime/trains (pas de CORS à gérer pour cette démo) — un site
    # tiers construisant sa propre carte appellerait /realtime/trains depuis son propre domaine,
    # cf. CORSMiddleware ci-dessus, avec sa propre clé API.
    # no-store : évite qu'un navigateur serve une version mise en cache après un rebuild du conteneur.
    html = (STATIC_DIR / "radar.html").read_text(encoding="utf-8")
    html = html.replace("{{CARTO_API_KEY}}", CARTO_API_KEY)
    return HTMLResponse(html, headers={"Cache-Control": "no-store"})


@app.get(
    "/realtime/trains",
    tags=["temps réel"],
    summary="Position estimée des trains actuellement en circulation",
    response_description=(
        "Une ligne par train actif : gares encadrantes, progression (0-1) et position GPS "
        "interpolée entre ces deux gares, retard en secondes, cause du retard si disponible "
        "(API SNCF officielle, `null` sinon), itinéraire restant (`itineraire_restant`, liste de "
        "[lat, lon] jusqu'au terminus). Conçu pour être sondé régulièrement (15-30s) afin "
        "d'animer une carte interactive."
    ),
    dependencies=[Depends(require_api_key)],
)
@limiter.limit("30/minute")
def realtime_trains(
    request: Request,
    max_age_s: int = Query(600, ge=30, le=3600, description="Ignore les trajets non mis à jour depuis plus de N secondes"),
):
    pg = get_pg_connection()
    try:
        rows = pg.execute(
            "SELECT trip_id, route_id, stops, updated_at FROM fact_realtime "
            "WHERE updated_at > now() - %s * interval '1 second'",
            [max_age_s],
        ).fetchall()
    finally:
        pg.close()

    stations = _stations_by_uic()
    disruptions = _active_disruptions_cached()
    now = int(time.time())
    results = []
    for trip_id, route_id, stops, updated_at in rows:
        position = _interpolate_trip(stops, now)
        if position is None:
            continue

        uic_prev = _uic_from_stop_id(position["stop_id_precedent"])
        uic_next = _uic_from_stop_id(position["stop_id_suivant"])
        gare_prev = stations.get(uic_prev)
        gare_next = stations.get(uic_next)
        if gare_prev is None or gare_next is None:
            continue  # gare non rapprochée du référentiel : pas de position inventée
        if uic_prev == uic_next:
            continue  # arrêts consécutifs à la même gare (doublon du flux, ex. sous-quais) : rien à interpoler

        fraction = position["progression"]
        latitude = gare_prev["latitude"] + (gare_next["latitude"] - gare_prev["latitude"]) * fraction
        longitude = gare_prev["longitude"] + (gare_next["longitude"] - gare_prev["longitude"]) * fraction

        train_number = _train_number_from_trip_id(trip_id)
        disruption = disruptions.get(train_number) if train_number else None

        results.append({
            "trip_id": trip_id,
            "route_id": route_id,
            "statut": position["statut"],
            "gare_precedente": gare_prev["nom_gare"],
            "gare_suivante": gare_next["nom_gare"],
            "progression": fraction,
            "latitude": round(latitude, 5),
            "longitude": round(longitude, 5),
            "retard_s": position["retard_s"],
            "cause_retard": disruption["cause"] if disruption else None,
            "itineraire_restant": _remaining_route_coords(stops, position["index_restant"], stations),
            "maj": updated_at.isoformat(),
        })

    return results
