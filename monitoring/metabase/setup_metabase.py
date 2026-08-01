"""Configure Metabase via son API REST après le premier démarrage.

Usage :
    python monitoring/metabase/setup_metabase.py

Ce script :
  1. Attend que Metabase soit prêt (health check)
  2. Crée le compte admin et complète le setup initial
  3. Ajoute la base DuckDB comme source de données
  4. Crée les questions SQL (ponctualité, tarifs, top liaisons)
  5. Assemble le dashboard "Baromètre ferroviaire SNCF"
"""

import time
import sys
import requests

BASE = "http://localhost:3001/api"
ADMIN_EMAIL = "admin@eurohub.local"
ADMIN_PASSWORD = "EuroHub2026!"
DB_PATH = "/environments/preprod/db_preprod.duckdb"


def wait_ready(timeout=300):
    print("Attente de Metabase", end="", flush=True)
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            r = requests.get(f"{BASE}/health", timeout=3)
            if r.status_code == 200 and r.json().get("status") == "ok":
                print(" OK")
                return True
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(5)
    print(" TIMEOUT")
    return False


def get_setup_token():
    r = requests.get(f"{BASE}/session/properties")
    r.raise_for_status()
    token = r.json().get("setup-token")
    if not token:
        print("Pas de setup-token — Metabase déjà configuré ou token expiré.")
        return None
    return token


def setup_admin(token):
    payload = {
        "token": token,
        "user": {
            "first_name": "Ivan",
            "last_name": "Mustiere",
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
            "site_name": "EuroMobilityDataHub",
        },
        "prefs": {
            "site_name": "EuroMobilityDataHub",
            "allow_tracking": False,
        },
    }
    r = requests.post(f"{BASE}/setup", json=payload)
    r.raise_for_status()
    session_id = r.json()["id"]
    print(f"Admin créé  |  session : {session_id[:8]}…")
    return session_id


def login():
    r = requests.post(f"{BASE}/session", json={
        "username": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD,
    })
    r.raise_for_status()
    sid = r.json()["id"]
    print(f"Connexion OK  |  session : {sid[:8]}…")
    return sid


def headers(session_id):
    return {"X-Metabase-Session": session_id, "Content-Type": "application/json"}


def add_duckdb(session_id):
    h = headers(session_id)
    # Chercher si le type duckdb est disponible
    r = requests.get(f"{BASE}/database/db-details-error-type", headers=h)
    payload = {
        "engine": "duckdb",
        "name": "EuroMobilityDataHub — preprod",
        "details": {"database_file": DB_PATH},
        "auto_run_queries": True,
        "is_full_sync": True,
    }
    r = requests.post(f"{BASE}/database", json=payload, headers=h)
    r.raise_for_status()
    db_id = r.json()["id"]
    print(f"Base DuckDB ajoutée  |  id={db_id}")
    return db_id


def wait_sync(session_id, db_id, timeout=120):
    """Attend que Metabase ait scanné les tables."""
    h = headers(session_id)
    requests.post(f"{BASE}/database/{db_id}/sync_schema", headers=h)
    print("Synchronisation des tables", end="", flush=True)
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = requests.get(f"{BASE}/database/{db_id}/metadata", headers=h)
        if r.status_code == 200:
            tables = r.json().get("tables", [])
            names = [t["name"] for t in tables]
            if "fact_regularite" in names:
                print(f" OK — tables : {names}")
                return True
        print(".", end="", flush=True)
        time.sleep(5)
    print(" TIMEOUT — les tables ne sont pas encore visibles.")
    return False


def create_question(session_id, db_id, name, sql, display="table"):
    h = headers(session_id)
    payload = {
        "name": name,
        "dataset_query": {
            "type": "native",
            "native": {"query": sql},
            "database": db_id,
        },
        "display": display,
        "visualization_settings": {},
    }
    r = requests.post(f"{BASE}/card", json=payload, headers=h)
    r.raise_for_status()
    card_id = r.json()["id"]
    print(f"  Question créée : [{card_id}] {name}")
    return card_id


def create_dashboard(session_id, name, description):
    h = headers(session_id)
    r = requests.post(f"{BASE}/dashboard", json={
        "name": name,
        "description": description,
    }, headers=h)
    r.raise_for_status()
    dash_id = r.json()["id"]
    print(f"Dashboard créé : [{dash_id}] {name}")
    return dash_id


def add_cards_to_dashboard(session_id, dash_id, cards):
    """PUT all cards at once (Metabase v0.40+ API)."""
    h = headers(session_id)
    payload = {"cards": [
        {
            "id": -(i + 1),
            "card_id": card_id,
            "row": row, "col": col,
            "size_x": size_x, "size_y": size_y,
            "series": [], "parameter_mappings": [],
        }
        for i, (card_id, row, col, size_x, size_y) in enumerate(cards)
    ]}
    r = requests.put(f"{BASE}/dashboard/{dash_id}/cards", json=payload, headers=h)
    r.raise_for_status()
    print(f"  {len(cards)} cartes ajoutées au dashboard {dash_id}")


QUESTIONS = [
    (
        "Ponctualité moyenne par type de ligne",
        """
SELECT type_ligne,
       COUNT(*)                          AS nb_mois_liaisons,
       ROUND(AVG(taux_ponctualite), 2)   AS ponctualite_moyenne,
       ROUND(AVG(taux_annulation), 2)    AS annulation_moyenne,
       ROUND(AVG(retard_moyen_tous_trains_arrivee_min), 2) AS retard_moyen_min
FROM fact_regularite
GROUP BY type_ligne
ORDER BY ponctualite_moyenne DESC
        """.strip(),
        "bar",
    ),
    (
        "Évolution annuelle de la ponctualité",
        """
SELECT SUBSTR(mois, 1, 4)              AS annee,
       type_ligne,
       ROUND(AVG(taux_ponctualite), 1) AS ponctualite_moyenne
FROM fact_regularite
WHERE taux_ponctualite IS NOT NULL
GROUP BY annee, type_ligne
ORDER BY annee, type_ligne
        """.strip(),
        "line",
    ),
    (
        "Top 10 liaisons les plus ponctuelles",
        """
SELECT type_ligne, axe_label,
       ROUND(AVG(taux_ponctualite), 2) AS ponctualite_moyenne,
       COUNT(*)                         AS nb_mois
FROM fact_regularite
WHERE taux_ponctualite IS NOT NULL
GROUP BY type_ligne, axe_label
HAVING COUNT(*) >= 12
ORDER BY ponctualite_moyenne DESC
LIMIT 10
        """.strip(),
        "table",
    ),
    (
        "Bottom 10 liaisons (moins ponctuelles)",
        """
SELECT type_ligne, axe_label,
       ROUND(AVG(taux_ponctualite), 2) AS ponctualite_moyenne,
       COUNT(*)                         AS nb_mois
FROM fact_regularite
WHERE taux_ponctualite IS NOT NULL
GROUP BY type_ligne, axe_label
HAVING COUNT(*) >= 12
ORDER BY ponctualite_moyenne ASC
LIMIT 10
        """.strip(),
        "table",
    ),
    (
        "Prix au km par type de ligne (médiane)",
        """
SELECT type_ligne,
       COUNT(*)                        AS nb_tarifs,
       ROUND(MEDIAN(prix_moyen_km), 3) AS prix_km_median,
       ROUND(MIN(prix_moyen_km), 3)    AS prix_km_min,
       ROUND(MAX(prix_moyen_km), 3)    AS prix_km_max
FROM fact_fares
WHERE prix_moyen_km IS NOT NULL
  AND classe = '2'
  AND profil_tarifaire = 'Tarif Normal'
GROUP BY type_ligne
ORDER BY type_ligne
        """.strip(),
        "bar",
    ),
    (
        "Prix au km vs Ponctualité par liaison (H2)",
        """
SELECT f.type_ligne,
       f.gare_origine || ' → ' || f.gare_destination AS liaison,
       f.distance_km,
       f.prix_moyen_km,
       ROUND(AVG(r.taux_ponctualite), 2) AS ponctualite_moyenne
FROM fact_fares f
JOIN fact_regularite r
  ON r.type_ligne = f.type_ligne
 AND r.axe_label  = f.axe_label_regularite
WHERE f.prix_moyen_km IS NOT NULL
  AND f.classe = '2'
  AND f.profil_tarifaire = 'Tarif Normal'
  AND r.taux_ponctualite IS NOT NULL
GROUP BY f.type_ligne, f.gare_origine, f.gare_destination,
         f.distance_km, f.prix_moyen_km
ORDER BY f.type_ligne, liaison
        """.strip(),
        "scatter",
    ),
]

LAYOUT = [
    # (row, col, size_x, size_y)
    (0,  0, 12, 6),   # Ponctualité par type — barre pleine largeur
    (6,  0, 18, 8),   # Évolution annuelle — ligne pleine largeur
    (14, 0,  9, 8),   # Top 10
    (14, 9,  9, 8),   # Bottom 10
    (22, 0, 12, 6),   # Prix médian par type
    (22, 12, 12, 8),  # Scatter H2
]


def main():
    if not wait_ready():
        sys.exit(1)

    token = get_setup_token()
    if token:
        session_id = setup_admin(token)
    else:
        session_id = login()

    db_id = add_duckdb(session_id)
    wait_sync(session_id, db_id)

    print("\nCréation des questions SQL…")
    card_ids = []
    for name, sql, display in QUESTIONS:
        cid = create_question(session_id, db_id, name, sql, display)
        card_ids.append(cid)

    print("\nAssemblage du dashboard…")
    dash_id = create_dashboard(
        session_id,
        "Baromètre ferroviaire SNCF",
        "Analyse comparative ponctualité et tarifs — TGV / TER / Intercités (C2.1.3)",
    )
    cards = [
        (card_id, row, col, sx, sy)
        for card_id, (row, col, sx, sy) in zip(card_ids, LAYOUT)
    ]
    add_cards_to_dashboard(session_id, dash_id, cards)

    print(f"\n✓ Dashboard prêt : http://localhost:3001/dashboard/{dash_id}")
    print(f"  Identifiants   : {ADMIN_EMAIL} / {ADMIN_PASSWORD}")


if __name__ == "__main__":
    main()
