"""Exporte le tableau de résultats consolidé (remplace le Tableau 2 du dossier).

Un ligne par liaison/région : ponctualité, annulation, distance (si rapprochée), prix/km réel
(si rapproché) — toutes les valeurs viennent de calculs réels sur données réelles.

Usage : python analysis/export_results.py [--env preprod|prod]
Écrit outputs/resultats.csv.
"""

import argparse
import os
import pathlib

import duckdb
import yaml
from dotenv import load_dotenv

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT_PATH = ROOT / "outputs" / "resultats.csv"

QUERY = """
    SELECT
        r.type_ligne,
        r.axe_label,
        r.nb_mois,
        r.taux_ponctualite_moyen,
        r.taux_annulation_moyen,
        l.distance_km,
        f.prix_moyen_km
    FROM (
        SELECT
            type_ligne,
            axe_label,
            COUNT(*) AS nb_mois,
            ROUND(AVG(taux_ponctualite), 2) AS taux_ponctualite_moyen,
            ROUND(AVG(taux_annulation), 2) AS taux_annulation_moyen
        FROM fact_regularite
        WHERE taux_ponctualite IS NOT NULL
        GROUP BY type_ligne, axe_label
        -- même seuil que analysis/queries.sql (Requête 2) : exclut les axes à faible historique
        HAVING COUNT(*) >= 12
    ) r
    LEFT JOIN dim_liaisons l
        ON l.type_ligne = r.type_ligne AND l.axe_label = r.axe_label
    LEFT JOIN (
        SELECT type_ligne, axe_label_regularite, ROUND(AVG(prix_moyen_km), 3) AS prix_moyen_km
        FROM fact_fares
        WHERE classe = '2' AND profil_tarifaire = 'Tarif Normal' AND prix_moyen_km IS NOT NULL
        GROUP BY type_ligne, axe_label_regularite
    ) f ON f.type_ligne = r.type_ligne AND f.axe_label_regularite = r.axe_label
    ORDER BY r.type_ligne, r.taux_ponctualite_moyen DESC
"""


def load_config(env: str) -> dict:
    with open(ROOT / "config" / f"{env}.yaml", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def run(env: str, output_path: pathlib.Path = OUTPUT_PATH) -> pathlib.Path:
    config = load_config(env)
    db_path = ROOT / config["database"]["path"]
    output_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path), read_only=True)
    n_rows = con.execute(f"SELECT COUNT(*) FROM ({QUERY})").fetchone()[0]
    con.execute(f"COPY ({QUERY}) TO '{output_path.as_posix()}' (HEADER, DELIMITER ',')")
    con.close()

    print(f"[export] {n_rows} lignes écrites dans {output_path}")
    return output_path


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser(description="Exporte le tableau de résultats consolidé (outputs/resultats.csv).")
    parser.add_argument("--env", choices=["preprod", "prod"], default=None,
                         help="Écrase APP_ENV (.env) pour cette exécution")
    args = parser.parse_args()
    run(args.env or os.getenv("APP_ENV", "preprod"))
