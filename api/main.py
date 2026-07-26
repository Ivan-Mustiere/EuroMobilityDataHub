"""API REST EuroMobilityDataHub — sert les données réelles produites par le pipeline.

Lecture seule sur la base DuckDB de l'environnement APP_ENV (dev|preprod|prod).
Lancer : uvicorn api.main:app --reload
"""

import os
import pathlib
from typing import Optional

import duckdb
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

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

app = FastAPI(
    title="EuroMobilityDataHub API",
    description="Ponctualité ferroviaire SNCF et référentiel des gares, à partir de données ouvertes ODbL.",
    version="0.1.0",
)


def get_connection() -> duckdb.DuckDBPyConnection:
    db_path = DB_PATHS.get(APP_ENV)
    if db_path is None or not db_path.exists():
        raise HTTPException(status_code=503, detail=f"Base introuvable pour l'environnement '{APP_ENV}' ({db_path})")
    return duckdb.connect(str(db_path), read_only=True)


def rows_to_dicts(rows: list, columns: list[str]) -> list[dict]:
    return [dict(zip(columns, row)) for row in rows]


@app.get("/health")
def health():
    return {"status": "ok", "env": APP_ENV}


@app.get("/stations")
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


@app.get("/stations/{station_id}")
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


@app.get("/regularite")
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


@app.get("/regularite/stats")
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
