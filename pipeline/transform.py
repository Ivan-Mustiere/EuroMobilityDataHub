"""Nettoyage et harmonisation des 3 sources SNCF (TGV, TER, Intercités) en un schéma commun.

Écrit le résultat dans la base DuckDB de l'environnement demandé (voir config/<env>.yaml).

Schéma harmonisé (table `fact_regularite`) :
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
GARES_CSV = RAW_DIR / "gares.csv"
TARIFS_TGV_CSV = RAW_DIR / "tarifs_tgv_ouigo.csv"
TARIFS_INTERCITES_CSV = RAW_DIR / "tarifs_intercites.csv"


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
        CREATE OR REPLACE TABLE fact_regularite AS
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
        CREATE OR REPLACE TABLE fact_regularite AS
        SELECT
            *,
            CASE WHEN nb_trains_circules > 0
                 THEN ROUND(100.0 * (nb_trains_circules - nb_trains_retard_arrivee) / nb_trains_circules, 2)
            END AS taux_ponctualite,
            CASE WHEN nb_trains_prevus > 0
                 THEN ROUND(100.0 * nb_trains_annules / nb_trains_prevus, 2)
            END AS taux_annulation
        FROM fact_regularite
    """)


def _normalize_station_name_expr(expr: str) -> str:
    """Expression SQL normalisant un nom de gare pour le rapprochement dim_stations <-> fact_regularite.

    Majuscules, accents retirés, tirets remplacés par des espaces, "ST" isolé étendu en "SAINT".
    Améliore le taux de rapprochement (~40 % -> ~57 % des noms distincts lors de l'exploration
    Jour 3) mais reste imparfait : gares combinées ("ALBI/RODEZ"), gares étrangères hors
    référentiel français, ou variantes trop éloignées ne seront jamais rapprochées — c'est
    volontaire (pas de fuzzy matching hasardeux qui pourrait rapprocher deux gares différentes).
    """
    normalized = f"UPPER(TRIM({expr}))"
    for accented, plain in [
        ("É", "E"), ("È", "E"), ("Ê", "E"), ("Ë", "E"),
        ("À", "A"), ("Â", "A"), ("Î", "I"), ("Ï", "I"),
        ("Ô", "O"), ("Û", "U"),
    ]:
        normalized = f"REPLACE({normalized}, '{accented}', '{plain}')"
    normalized = f"REGEXP_REPLACE({normalized}, '-', ' ', 'g')"
    normalized = f"REGEXP_REPLACE({normalized}, '\\bST\\b', 'SAINT', 'g')"
    return normalized


def build_dim_stations(con: duckdb.DuckDBPyConnection, gares_csv: pathlib.Path = GARES_CSV) -> None:
    """Référentiel des gares SNCF (nom aligné sur le Bloc 1 : dim_stations).

    "Position géographique" est une seule colonne "lat, lon" dans le CSV source : on la
    découpe en deux colonnes numériques. `nom_gare_norm` sert de clé de rapprochement avec les
    libellés de gare de fact_regularite (voir _normalize_station_name_expr et build_dim_liaisons) —
    limite connue : le rapprochement par nom reste partiel (abréviations, gares combinées,
    gares étrangères) ; à documenter en 4.4/6.2.
    """
    nom_gare_norm = _normalize_station_name_expr('"Nom_Gare"')
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_stations AS
        SELECT
            "Id_Gare" AS station_id,
            "Nom_Gare" AS nom_gare,
            {nom_gare_norm} AS nom_gare_norm,
            "Trigramme" AS trigramme,
            "Code_UIC" AS code_uic,
            "Code commune" AS code_commune,
            TRY_CAST(SPLIT_PART("Position géographique", ',', 1) AS DOUBLE) AS latitude,
            TRY_CAST(TRIM(SPLIT_PART("Position géographique", ',', 2)) AS DOUBLE) AS longitude
        FROM read_csv('{pathlib.Path(gares_csv).as_posix()}', delim=';', header=true)
    """)


def build_dim_liaisons(con: duckdb.DuckDBPyConnection) -> None:
    """Distance à vol d'oiseau (Haversine, km) par liaison TGV/Intercités, via dim_stations.

    Ne concerne que les axes de type 'liaison' (TGV, Intercités) — le TER est agrégé par région,
    pas par liaison, donc pas de distance calculable. `distance_km` est NULL quand l'une des deux
    gares n'a pas été rapprochée de dim_stations : on ne comble jamais par une valeur approximative
    ou inventée (cf. contrainte non négociable du projet).
    """
    depart_norm = _normalize_station_name_expr("l.gare_depart")
    arrivee_norm = _normalize_station_name_expr("l.gare_arrivee")
    station_norm = _normalize_station_name_expr("nom_gare")

    con.execute(f"""
        CREATE OR REPLACE TABLE dim_liaisons AS
        WITH liaisons AS (
            SELECT DISTINCT type_ligne, axe_label, gare_depart, gare_arrivee
            FROM fact_regularite
            WHERE axe_type = 'liaison'
        ),
        stations_dedup AS (
            -- un même nom normalisé peut correspondre à plusieurs points très proches (quais,
            -- annexes) : on moyenne les coordonnées plutôt que d'en choisir une arbitrairement
            SELECT {station_norm} AS nom_norm, AVG(latitude) AS latitude, AVG(longitude) AS longitude
            FROM dim_stations
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            GROUP BY {station_norm}
        )
        SELECT
            l.type_ligne,
            l.axe_label,
            l.gare_depart,
            l.gare_arrivee,
            d.latitude  AS latitude_depart,
            d.longitude AS longitude_depart,
            a.latitude  AS latitude_arrivee,
            a.longitude AS longitude_arrivee,
            CASE WHEN d.latitude IS NOT NULL AND a.latitude IS NOT NULL THEN
                ROUND(
                    6371 * 2 * ASIN(SQRT(
                        POWER(SIN(RADIANS(a.latitude - d.latitude) / 2), 2) +
                        COS(RADIANS(d.latitude)) * COS(RADIANS(a.latitude)) *
                        POWER(SIN(RADIANS(a.longitude - d.longitude) / 2), 2)
                    )), 1
                )
            END AS distance_km
        FROM liaisons l
        LEFT JOIN stations_dedup d ON d.nom_norm = {depart_norm}
        LEFT JOIN stations_dedup a ON a.nom_norm = {arrivee_norm}
    """)


def build_fact_fares(
    con: duckdb.DuckDBPyConnection,
    tgv_fares_csv: pathlib.Path = TARIFS_TGV_CSV,
    intercites_fares_csv: pathlib.Path = TARIFS_INTERCITES_CSV,
) -> None:
    """Grilles tarifaires officielles SNCF (nom aligné sur le Bloc 1 : fact_fares).

    Sources ouvertes ODbL (data.gouv.fr) : tarifs TGV INOUI/OUIGO et tarifs Intercités, prix
    minimum/maximum par origine-destination-classe-profil tarifaire. **Remplace la collecte
    manuelle initialement prévue** (captures d'écran SNCF Connect, partie 3.1 du dossier) —
    décision prise en session Claude Code Jour 3 : une grille officielle réutilisable et
    reproductible par le pipeline est plus rigoureuse qu'un relevé manuel ponctuel, à documenter
    comme un changement de méthode par rapport au dossier existant.

    prix_moyen = (prix_minimum + prix_maximum) / 2. distance_km / prix_moyen_km viennent du
    rapprochement avec dim_liaisons (même limite de couverture que celle-ci : NULL si la liaison
    correspondante n'a pas de distance calculée — pas de valeur inventée).
    """
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_fares AS
        SELECT
            CASE WHEN "Transporteur" IN ('TGV INOUI', 'OUIGO', 'OUIGO TRAIN CLASSIQUE')
                 THEN 'grande_vitesse' ELSE 'intercite' END AS type_ligne,
            "Transporteur" AS transporteur,
            "Gare origine" AS gare_origine,
            "Gare destination" AS gare_destination,
            "Classe" AS classe,
            "Profil tarifaire" AS profil_tarifaire,
            TRY_CAST("Prix minimum" AS DOUBLE) AS prix_minimum,
            TRY_CAST("Prix maximum" AS DOUBLE) AS prix_maximum,
            'EUR' AS devise,
            CURRENT_DATE AS date_collecte,
            'tarifs_tgv_ouigo.csv' AS source_file
        FROM read_csv('{pathlib.Path(tgv_fares_csv).as_posix()}', delim=';', header=true)

        UNION ALL BY NAME

        SELECT
            'intercite' AS type_ligne,
            "Transporteur" AS transporteur,
            "Gare origine" AS gare_origine,
            "Gare destination" AS gare_destination,
            "Classe" AS classe,
            "Profil tarifaire" AS profil_tarifaire,
            TRY_CAST("Prix minimum" AS DOUBLE) AS prix_minimum,
            TRY_CAST("Prix maximum" AS DOUBLE) AS prix_maximum,
            'EUR' AS devise,
            CURRENT_DATE AS date_collecte,
            'tarifs_intercites.csv' AS source_file
        FROM read_csv('{pathlib.Path(intercites_fares_csv).as_posix()}', delim=';', header=true)
    """)

    con.execute("""
        CREATE OR REPLACE TABLE fact_fares AS
        SELECT *, ROUND((prix_minimum + prix_maximum) / 2, 2) AS prix_moyen
        FROM fact_fares
    """)

    origine_norm = _normalize_station_name_expr("f.gare_origine")
    destination_norm = _normalize_station_name_expr("f.gare_destination")
    liaison_depart_norm = _normalize_station_name_expr("l.gare_depart")
    liaison_arrivee_norm = _normalize_station_name_expr("l.gare_arrivee")

    con.execute(f"""
        CREATE OR REPLACE TABLE fact_fares AS
        SELECT
            f.*,
            l.axe_label AS axe_label_regularite,
            l.distance_km,
            CASE WHEN l.distance_km > 0 THEN ROUND(f.prix_moyen / l.distance_km, 3) END AS prix_moyen_km
        FROM fact_fares f
        LEFT JOIN dim_liaisons l
            ON f.type_ligne = l.type_ligne
           AND {origine_norm} = {liaison_depart_norm}
           AND {destination_norm} = {liaison_arrivee_norm}
    """)


def apply_dev_sample(con: duckdb.DuckDBPyConnection, sample_cfg: dict) -> None:
    liaisons_max = sample_cfg.get("liaisons_max", 1)
    mois_max = sample_cfg.get("mois_max", 3)
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_regularite AS
        WITH mois_max_par_type AS (
            SELECT type_ligne, MAX(mois) AS m FROM fact_regularite GROUP BY type_ligne
        ),
        axes_actifs AS (
            -- ne garder que des axes qui ont encore des données au dernier mois disponible
            -- pour LEUR type de ligne (les 3 sources n'ont pas toutes la même couverture
            -- temporelle) ; évite par ex. une région TER disparue lors de la fusion des
            -- régions de 2016
            SELECT DISTINCT r.type_ligne, r.axe_label
            FROM fact_regularite r
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
            FROM fact_regularite
            ORDER BY mois DESC
            LIMIT {mois_max}
        )
        SELECT r.*
        FROM fact_regularite r
        WHERE (r.type_ligne, r.axe_label) IN (SELECT type_ligne, axe_label FROM axes_retenus)
          AND r.mois IN (SELECT mois FROM mois_recents)
    """)


def run(env: str) -> None:
    config = load_config(env)
    db_path = ROOT / config["database"]["path"]
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    build_harmonized_table(con)

    n_total = con.execute("SELECT COUNT(*) FROM fact_regularite").fetchone()[0]
    print(f"[transform] table 'fact_regularite' construite : {n_total} lignes (avant échantillonnage éventuel)")

    if config.get("sample", {}).get("enabled"):
        apply_dev_sample(con, config["sample"])
        n_sample = con.execute("SELECT COUNT(*) FROM fact_regularite").fetchone()[0]
        print(f"[transform] échantillon dev appliqué : {n_sample} lignes")

    build_dim_stations(con)
    n_stations = con.execute("SELECT COUNT(*) FROM dim_stations").fetchone()[0]
    print(f"[transform] table 'dim_stations' construite : {n_stations} gares")

    build_dim_liaisons(con)
    n_liaisons, n_avec_distance = con.execute(
        "SELECT COUNT(*), SUM(CASE WHEN distance_km IS NOT NULL THEN 1 ELSE 0 END) FROM dim_liaisons"
    ).fetchone()
    print(f"[transform] table 'dim_liaisons' construite : {n_avec_distance}/{n_liaisons} liaisons avec distance calculée")

    build_fact_fares(con)
    n_tarifs, n_avec_prix_km = con.execute(
        "SELECT COUNT(*), SUM(CASE WHEN prix_moyen_km IS NOT NULL THEN 1 ELSE 0 END) FROM fact_fares"
    ).fetchone()
    print(f"[transform] table 'fact_fares' construite : {n_tarifs} tarifs, {n_avec_prix_km} avec prix/km calculé")

    con.close()
    print(f"[transform] base écrite dans {db_path}")


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(description="Nettoie et harmonise les 3 CSV SNCF dans la base DuckDB de l'environnement.")
    parser.add_argument("--env", choices=["dev", "preprod", "prod"], default=None,
                         help="Écrase APP_ENV (.env) pour cette exécution")
    args = parser.parse_args()
    run(args.env or os.getenv("APP_ENV", "dev"))
