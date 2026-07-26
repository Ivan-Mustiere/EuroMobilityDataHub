"""Nettoyage et harmonisation des 3 sources SNCF (TGV, TER, Intercités) en un schéma commun.

Écrit le résultat dans la base DuckDB de l'environnement demandé (voir config/<env>.yaml).

Schéma harmonisé (table `regularite`) :
    mois                                    VARCHAR   'YYYY-MM'
    type_ligne                              VARCHAR   'grande_vitesse' | 'regional' | 'intercite'
    axe_type                                VARCHAR   'liaison' (TGV/Intercités) | 'region' (TER)
    gare_depart / gare_arrivee              VARCHAR   NULL pour TER (granularité région, pas liaison)
    region                                  VARCHAR   NULL sauf TER
    axe_label                               VARCHAR   libellé lisible de l'axe (liaison ou région)
    nb_trains_prevus                        INTEGER
    nb_trains_circules                      INTEGER
    nb_trains_annules                       INTEGER
    nb_trains_retard_arrivee                INTEGER
    retard_moyen_tous_trains_arrivee_min    DOUBLE    disponible uniquement pour TGV (voir limite ci-dessous)
    taux_ponctualite                        DOUBLE    % = (circules - retard) / circules * 100, recalculé uniformément
    taux_annulation                         DOUBLE    % = annules / prevus * 100
    source_file                             VARCHAR

Limite documentée (à reporter en partie 4.4/6.2 du dossier) : seule la source TGV fournit un
retard moyen en minutes ("Retard moyen de tous les trains à l'arrivée"). TER et Intercités ne
publient qu'un taux de régularité agrégé (pas de retard moyen ni de P90 par train) : l'indicateur
"retard moyen" ne sera donc calculable finement que sur l'axe grande vitesse.
"""

import argparse
import os
import pathlib

import duckdb
import yaml
from dotenv import load_dotenv

ROOT = pathlib.Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"

TGV_CSV = RAW_DIR / "regularite_tgv.csv"
TER_CSV = RAW_DIR / "regularite_ter.csv"
INTERCITES_CSV = RAW_DIR / "regularite_intercites.csv"


def load_config(env: str) -> dict:
    config_path = ROOT / "config" / f"{env}.yaml"
    with open(config_path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def build_harmonized_table(
    con: duckdb.DuckDBPyConnection,
    tgv_csv: pathlib.Path = TGV_CSV,
    ter_csv: pathlib.Path = TER_CSV,
    intercites_csv: pathlib.Path = INTERCITES_CSV,
) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE regularite AS
        SELECT
            "Date" AS mois,
            'grande_vitesse' AS type_ligne,
            'liaison' AS axe_type,
            "Gare de départ" AS gare_depart,
            "Gare d'arrivée" AS gare_arrivee,
            CAST(NULL AS VARCHAR) AS region,
            "Gare de départ" || ' -> ' || "Gare d'arrivée" AS axe_label,
            TRY_CAST("Nombre de circulations prévues" AS INTEGER) AS nb_trains_prevus,
            TRY_CAST("Nombre de circulations prévues" AS INTEGER) - TRY_CAST("Nombre de trains annulés" AS INTEGER) AS nb_trains_circules,
            TRY_CAST("Nombre de trains annulés" AS INTEGER) AS nb_trains_annules,
            TRY_CAST("Nombre de trains en retard à l'arrivée" AS INTEGER) AS nb_trains_retard_arrivee,
            TRY_CAST("Retard moyen de tous les trains à l'arrivée" AS DOUBLE) AS retard_moyen_tous_trains_arrivee_min,
            'regularite_tgv.csv' AS source_file
        FROM read_csv('{pathlib.Path(tgv_csv).as_posix()}', delim=';', header=true)

        UNION ALL BY NAME

        SELECT
            "Date" AS mois,
            'regional' AS type_ligne,
            'region' AS axe_type,
            CAST(NULL AS VARCHAR) AS gare_depart,
            CAST(NULL AS VARCHAR) AS gare_arrivee,
            "Région" AS region,
            "Région" AS axe_label,
            TRY_CAST("Nombre de trains programmés" AS INTEGER) AS nb_trains_prevus,
            TRY_CAST("Nombre de trains ayant circulé" AS INTEGER) AS nb_trains_circules,
            TRY_CAST("Nombre de trains annulés" AS INTEGER) AS nb_trains_annules,
            TRY_CAST("Nombre de trains en retard à l'arrivée" AS INTEGER) AS nb_trains_retard_arrivee,
            CAST(NULL AS DOUBLE) AS retard_moyen_tous_trains_arrivee_min,
            'regularite_ter.csv' AS source_file
        FROM read_csv('{pathlib.Path(ter_csv).as_posix()}', delim=';', header=true)

        UNION ALL BY NAME

        SELECT
            "Date" AS mois,
            'intercite' AS type_ligne,
            'liaison' AS axe_type,
            "Départ" AS gare_depart,
            "Arrivée" AS gare_arrivee,
            CAST(NULL AS VARCHAR) AS region,
            "Départ" || ' -> ' || "Arrivée" AS axe_label,
            TRY_CAST("Nombre de trains programmés" AS INTEGER) AS nb_trains_prevus,
            TRY_CAST("Nombre de trains ayant circulé" AS INTEGER) AS nb_trains_circules,
            TRY_CAST("Nombre de trains annulés" AS INTEGER) AS nb_trains_annules,
            TRY_CAST("Nombre de trains en retard à l'arrivée" AS INTEGER) AS nb_trains_retard_arrivee,
            CAST(NULL AS DOUBLE) AS retard_moyen_tous_trains_arrivee_min,
            'regularite_intercites.csv' AS source_file
        FROM read_csv('{pathlib.Path(intercites_csv).as_posix()}', delim=';', header=true)
    """)

    con.execute("""
        CREATE OR REPLACE TABLE regularite AS
        SELECT
            *,
            CASE WHEN nb_trains_circules > 0
                 THEN ROUND(100.0 * (nb_trains_circules - nb_trains_retard_arrivee) / nb_trains_circules, 2)
            END AS taux_ponctualite,
            CASE WHEN nb_trains_prevus > 0
                 THEN ROUND(100.0 * nb_trains_annules / nb_trains_prevus, 2)
            END AS taux_annulation
        FROM regularite
    """)


def apply_dev_sample(con: duckdb.DuckDBPyConnection, sample_cfg: dict) -> None:
    liaisons_max = sample_cfg.get("liaisons_max", 1)
    mois_max = sample_cfg.get("mois_max", 3)
    con.execute(f"""
        CREATE OR REPLACE TABLE regularite AS
        WITH mois_max_par_type AS (
            SELECT type_ligne, MAX(mois) AS m FROM regularite GROUP BY type_ligne
        ),
        axes_actifs AS (
            -- ne garder que des axes qui ont encore des données au dernier mois disponible
            -- pour LEUR type de ligne (les 3 sources n'ont pas toutes la même couverture
            -- temporelle) ; évite par ex. une région TER disparue lors de la fusion des
            -- régions de 2016
            SELECT DISTINCT r.type_ligne, r.axe_label
            FROM regularite r
            JOIN mois_max_par_type mmt ON mmt.type_ligne = r.type_ligne AND mmt.m = r.mois
        ),
        axes_par_type AS (
            SELECT type_ligne, axe_label,
                   ROW_NUMBER() OVER (PARTITION BY type_ligne ORDER BY axe_label) AS rn
            FROM axes_actifs
        ),
        axes_retenus AS (
            SELECT type_ligne, axe_label FROM axes_par_type WHERE rn <= {liaisons_max}
        ),
        mois_recents AS (
            SELECT DISTINCT mois
            FROM regularite
            ORDER BY mois DESC
            LIMIT {mois_max}
        )
        SELECT r.*
        FROM regularite r
        WHERE (r.type_ligne, r.axe_label) IN (SELECT type_ligne, axe_label FROM axes_retenus)
          AND r.mois IN (SELECT mois FROM mois_recents)
    """)


def run(env: str) -> None:
    config = load_config(env)
    db_path = ROOT / config["database"]["path"]
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    build_harmonized_table(con)

    n_total = con.execute("SELECT COUNT(*) FROM regularite").fetchone()[0]
    print(f"[transform] table 'regularite' construite : {n_total} lignes (avant échantillonnage éventuel)")

    if config.get("sample", {}).get("enabled"):
        apply_dev_sample(con, config["sample"])
        n_sample = con.execute("SELECT COUNT(*) FROM regularite").fetchone()[0]
        print(f"[transform] échantillon dev appliqué : {n_sample} lignes")

    con.close()
    print(f"[transform] base écrite dans {db_path}")


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(description="Nettoie et harmonise les 3 CSV SNCF dans la base DuckDB de l'environnement.")
    parser.add_argument("--env", choices=["dev", "preprod", "prod"], default=None,
                         help="Écrase APP_ENV (.env) pour cette exécution")
    args = parser.parse_args()
    run(args.env or os.getenv("APP_ENV", "dev"))
