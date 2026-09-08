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
import zipfile

import duckdb
import yaml
from dotenv import load_dotenv

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
RAW_DIR = ROOT / "data" / "raw"

TGV_CSV = RAW_DIR / "regularite_tgv.csv"
TER_CSV = RAW_DIR / "regularite_ter.csv"
INTERCITES_CSV = RAW_DIR / "regularite_intercites.csv"
GARES_CSV = RAW_DIR / "gares.csv"
TARIFS_TGV_CSV = RAW_DIR / "tarifs_tgv_ouigo.csv"
TARIFS_INTERCITES_CSV = RAW_DIR / "tarifs_intercites.csv"
CH_GTFS_ZIP = RAW_DIR / "ch_gtfs_static.zip"
NL_GTFS_ZIP = RAW_DIR / "nl_gtfs_static.zip"
IT_GTFS_ZIP = RAW_DIR / "it_gtfs_static.zip"
FI_STATIONS_JSON = RAW_DIR / "fi_stations.json"
PL_GTFS_ZIP = RAW_DIR / "pl_gtfs_static.zip"
DE_GTFS_ZIP = RAW_DIR / "de_gtfs_static.zip"
SE_GTFS_ZIP = RAW_DIR / "se_gtfs_static.zip"
# Vocabulaire GTFS "basic route types" (pas la variante étendue européenne utilisée par la Suisse) :
# 2 = Rail. Cf. build_de_reference.
DE_RAIL_ROUTE_TYPE = "2"

# Vocabulaire GTFS "extended route types" (norme européenne) : 100-117 = famille rail
# (100 Railway, 101 High Speed, 102 Long Distance, 103 Inter Regional, 105 Sleeper, 106 Regional,
# 107 Tourist, 109 Suburban, 116 Rack/Pinion, 117 Additional Rail...). Le flux GTFS-RT suisse
# mélange tous les modes (bus/tram/rail) ; ce filtre sert à ne garder que les trains
# (cf. apps/streaming/producer.py, RAIL_ROUTES_FILE).
CH_RAIL_ROUTE_TYPES = set(range(100, 118))


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
            TRY_CAST(TRIM(SPLIT_PART("Position géographique", ',', 2)) AS DOUBLE) AS longitude,
            'FR' AS pays
        FROM read_csv('{pathlib.Path(gares_csv).as_posix()}', delim=';', header=true)
    """)


def build_ch_reference(con: duckdb.DuckDBPyConnection, gtfs_zip: pathlib.Path = CH_GTFS_ZIP) -> None:
    """Référentiel suisse (gares + lignes ferroviaires), pour le temps réel CH.

    Contrairement à la SNCF, le flux GTFS-RT suisse ne publie quasiment jamais le code UIC
    directement dans le `stop_id` (format "sloid" pour 165 932 arrêts sur ~166 000 testés lors de
    l'exploration) : il faut le référentiel statique (stops.txt) pour le résoudre, via sa colonne
    `didok` (code UIC/DiDok classique, présent pour toute variante de stop_id, y compris les
    "sloid" au niveau quai). Ce même flux mélange aussi tous les modes de transport (bus/tram/
    rail) : routes.txt (route_type, vocabulaire GTFS étendu européen) sert à isoler les trains.

    Si `gtfs_zip` est absent, ne fait rien (pas d'échec bloquant : la Suisse est un ajout, pas un
    prérequis du pipeline existant).
    """
    if not gtfs_zip.exists():
        print(f"[transform] {gtfs_zip} absent, référentiel suisse ignoré (voir apps/pipeline/ingest.py)")
        return

    stops_csv = RAW_DIR / "ch_stops.csv"
    routes_csv = RAW_DIR / "ch_routes.csv"
    agency_csv = RAW_DIR / "ch_agency.csv"
    with zipfile.ZipFile(gtfs_zip) as z:
        for member, dest in [("stops.txt", stops_csv), ("routes.txt", routes_csv), ("agency.txt", agency_csv)]:
            with z.open(member) as src, open(dest, "wb") as out:
                out.write(src.read())

    # Limite documentée : stops.txt couvre TOUS les arrêts suisses ayant un code didok (bus, tram,
    # rail confondus) — on ne peut isoler les seuls arrêts ferroviaires qu'via stop_times.txt
    # (3 Go non chargé ici, cf. plan Suisse). Sans conséquence pour le temps réel (seuls des
    # codes UIC réellement référencés par un trajet ferroviaire sont interrogés), mais /stations
    # et /stations/{id} exposeront aussi des arrêts de bus/tram suisses (pays='CH').
    #
    # Une gare par code `didok` (comme `stations_dedup` dans build_dim_liaisons pour les
    # collisions de nom : on moyenne les coordonnées des variantes plutôt que d'en choisir une
    # arbitrairement — la France utilise la même logique de dédoublonnage).
    con.execute(f"""
        INSERT INTO dim_stations
        SELECT
            didok AS station_id,
            FIRST(stop_name) AS nom_gare,
            {_normalize_station_name_expr("FIRST(stop_name)")} AS nom_gare_norm,
            NULL AS trigramme,
            didok AS code_uic,
            NULL AS code_commune,
            AVG(TRY_CAST(stop_lat AS DOUBLE)) AS latitude,
            AVG(TRY_CAST(stop_lon AS DOUBLE)) AS longitude,
            'CH' AS pays
        FROM read_csv('{stops_csv.as_posix()}', header=true, types={{'didok': 'VARCHAR', 'stop_id': 'VARCHAR'}})
        WHERE didok IS NOT NULL AND didok != ''
        GROUP BY didok
    """)

    # Table de résolution stop_id (brut, y compris "sloid" au niveau quai) -> code UIC : c'est
    # elle, pas dim_stations, que /realtime/trains consulte pour rapprocher un stop_id GTFS-RT
    # suisse d'une gare (cf. apps/api/main.py, _uic_from_stop_id).
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_stop_uic_ch AS
        SELECT stop_id, didok AS code_uic
        FROM read_csv('{stops_csv.as_posix()}', header=true, types={{'didok': 'VARCHAR', 'stop_id': 'VARCHAR'}})
        WHERE didok IS NOT NULL AND didok != ''
    """)

    rail_route_types = ",".join(str(t) for t in sorted(CH_RAIL_ROUTE_TYPES))
    rail_route_ids = con.execute(f"""
        SELECT route_id
        FROM read_csv('{routes_csv.as_posix()}', header=true)
        WHERE TRY_CAST(route_type AS INTEGER) IN ({rail_route_types})
    """).fetchall()
    rail_routes_file = RAW_DIR / "ch_rail_routes.txt"
    rail_routes_file.write_text("\n".join(row[0] for row in rail_route_ids) + "\n", encoding="utf-8")
    print(f"[transform] {rail_routes_file} écrit ({len(rail_route_ids)} lignes ferroviaires)")

    # Libellé de ligne (ex. "S10", "IC1") par route_id, pour l'affichage sur la carte (radar.html,
    # routeLabel) — le route_id du flux temps réel suisse (ex. "91-71A-j26-1") n'est pas lisible
    # tel quel, contrairement au régime français où route_id est souvent absent et remplacé par un
    # nom de gamme déduit du trip_id (TGV INOUI, TER...).
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_route_label_ch AS
        SELECT route_id, route_short_name AS label
        FROM read_csv('{routes_csv.as_posix()}', header=true)
        WHERE route_short_name IS NOT NULL AND route_short_name != ''
    """)

    # Opérateur (ex. "Schweizerische Bundesbahnen SBB", "THURBO") par route_id, pour l'affichage
    # "qui gère ce train" sur la carte (radar.html) — la Suisse a de nombreux opérateurs
    # régionaux distincts, contrairement à la France (toujours SNCF, cf. apps/api/main.py).
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_route_operator_ch AS
        SELECT r.route_id, a.agency_name AS operateur
        FROM read_csv('{routes_csv.as_posix()}', header=true, types={{'agency_id': 'VARCHAR'}}) r
        JOIN read_csv('{agency_csv.as_posix()}', header=true, types={{'agency_id': 'VARCHAR'}}) a
          ON a.agency_id = r.agency_id
    """)


def build_nl_reference(con: duckdb.DuckDBPyConnection, gtfs_zip: pathlib.Path = NL_GTFS_ZIP) -> None:
    """Référentiel néerlandais (gares), pour le temps réel NL.

    Bien plus simple que la Suisse (cf. build_ch_reference) : `trainUpdates.pb` (utilisé par
    apps/streaming/producer.py) est déjà un flux dédié aux trains — contrairement au flux suisse
    qui mélange tous les modes, pas besoin de filtrer par type de ligne. Et 99,8% des `stop_id`
    (35783/35841 événements testés lors de l'exploration) sont déjà des codes à 7-8 chiffres que
    la regex existante (_STOP_ID_UIC_RE, apps/api/main.py) sait extraire directement : pas besoin
    d'une table de résolution séparée comme dim_stop_uic_ch. Limite connue et acceptée : les ~0,2%
    de stop_id à 6 chiffres ne sont pas rapprochés (pas de position inventée).

    Si `gtfs_zip` est absent, ne fait rien (pas d'échec bloquant : ajout, pas un prérequis).
    """
    if not gtfs_zip.exists():
        print(f"[transform] {gtfs_zip} absent, référentiel néerlandais ignoré (voir apps/pipeline/ingest.py)")
        return

    stops_csv = RAW_DIR / "nl_stops.csv"
    with zipfile.ZipFile(gtfs_zip) as z, z.open("stops.txt") as src, open(stops_csv, "wb") as out:
        out.write(src.read())

    con.execute(f"""
        INSERT INTO dim_stations
        SELECT
            stop_id AS station_id,
            stop_name AS nom_gare,
            {_normalize_station_name_expr("stop_name")} AS nom_gare_norm,
            NULL AS trigramme,
            stop_id AS code_uic,
            NULL AS code_commune,
            TRY_CAST(stop_lat AS DOUBLE) AS latitude,
            TRY_CAST(stop_lon AS DOUBLE) AS longitude,
            'NL' AS pays
        FROM read_csv('{stops_csv.as_posix()}', header=true, types={{'stop_id': 'VARCHAR'}})
        WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL
    """)

    # Tracé réel des voies (shapes.txt), pour dessiner le trajet d'un train sélectionné en suivant
    # la géométrie réelle plutôt que des segments droits entre gares. Le trip_id du flux temps réel
    # (apps/streaming/producer.py) ne correspond exactement au trip_id de trips.txt que pour ~91%
    # des trains observés lors de l'exploration (le reste utilise un format préfixé par la date de
    # service, ex. "2026-09-07:IFF:S:318953", sans correspondance fiable) — limite acceptée, pas de
    # correspondance approximative risquée : les ~9% restants gardent le tracé en ligne droite.
    routes_csv = RAW_DIR / "nl_routes.csv"
    trips_csv = RAW_DIR / "nl_trips.csv"
    shapes_csv = RAW_DIR / "nl_shapes.csv"
    agency_csv = RAW_DIR / "nl_agency.csv"
    with zipfile.ZipFile(gtfs_zip) as z:
        for member, dest in [("routes.txt", routes_csv), ("trips.txt", trips_csv),
                              ("shapes.txt", shapes_csv), ("agency.txt", agency_csv)]:
            with z.open(member) as src, open(dest, "wb") as out:
                out.write(src.read())

    con.execute(f"""
        CREATE OR REPLACE TABLE dim_trip_shape_nl AS
        SELECT t.trip_id, t.shape_id
        FROM read_csv('{trips_csv.as_posix()}', header=true,
                       types={{'trip_id': 'VARCHAR', 'shape_id': 'VARCHAR', 'route_id': 'VARCHAR'}}) t
        JOIN read_csv('{routes_csv.as_posix()}', header=true, types={{'route_id': 'VARCHAR'}}) r
          ON r.route_id = t.route_id
        WHERE r.route_type = '2' AND t.shape_id IS NOT NULL AND t.shape_id != ''
    """)

    # Libellé de gamme (ex. "Intercity", "Sprinter"), pour l'affichage sur la carte (radar.html,
    # routeLabel) — sans lien avec le tracé réel ci-dessus (couverture différente : disponible pour
    # la quasi-totalité des trajets, contrairement aux ~91% ayant un shape_id).
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_trip_label_nl AS
        SELECT t.trip_id, t.trip_long_name AS label
        FROM read_csv('{trips_csv.as_posix()}', header=true,
                       types={{'trip_id': 'VARCHAR', 'route_id': 'VARCHAR'}}) t
        JOIN read_csv('{routes_csv.as_posix()}', header=true, types={{'route_id': 'VARCHAR'}}) r
          ON r.route_id = t.route_id
        WHERE r.route_type = '2' AND t.trip_long_name IS NOT NULL AND t.trip_long_name != ''
    """)

    # Opérateur (ex. "NS", "Arriva", "Keolis") par route_id, pour l'affichage "qui gère ce train"
    # sur la carte (radar.html) — les Pays-Bas ont plusieurs opérateurs régionaux, contrairement à
    # la France (toujours SNCF, cf. apps/api/main.py).
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_route_operator_nl AS
        SELECT DISTINCT r.route_id, a.agency_name AS operateur
        FROM read_csv('{routes_csv.as_posix()}', header=true, types={{'agency_id': 'VARCHAR', 'route_id': 'VARCHAR'}}) r
        JOIN read_csv('{agency_csv.as_posix()}', header=true, types={{'agency_id': 'VARCHAR'}}) a
          ON a.agency_id = r.agency_id
        WHERE r.route_type = '2'
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE dim_shapes_nl AS
        SELECT
            shape_id,
            list([TRY_CAST(shape_pt_lat AS DOUBLE), TRY_CAST(shape_pt_lon AS DOUBLE)]
                 ORDER BY TRY_CAST(shape_pt_sequence AS INTEGER)) AS points
        FROM read_csv('{shapes_csv.as_posix()}', header=true, types={{'shape_id': 'VARCHAR'}})
        WHERE shape_id IN (SELECT DISTINCT shape_id FROM dim_trip_shape_nl)
        GROUP BY shape_id
    """)


def build_it_reference(con: duckdb.DuckDBPyConnection, gtfs_zip: pathlib.Path = IT_GTFS_ZIP) -> None:
    """Référentiel du flux temps réel Trenitalia France (trains transfrontaliers Paris/Lyon-Milan,
    pas le réseau italien domestique — aucun flux GTFS-RT national italien ouvert n'a été trouvé),
    pour le temps réel IT.

    Le plus simple des trois référentiels étrangers : ~20 gares seulement, un seul opérateur
    (Trenitalia), stop_id déjà directement exploitable (pas de format "sloid" comme la Suisse) —
    mais trop court (5 chiffres) pour la regex UIC existante (_STOP_ID_UIC_RE, 7-8 chiffres,
    apps/api/main.py) : dim_stop_uic_it fournit quand même la résolution, par simple identité.

    Si `gtfs_zip` est absent, ne fait rien (pas d'échec bloquant : ajout, pas un prérequis).
    """
    if not gtfs_zip.exists():
        print(f"[transform] {gtfs_zip} absent, référentiel italien ignoré (voir apps/pipeline/ingest.py)")
        return

    stops_csv = RAW_DIR / "it_stops.csv"
    routes_csv = RAW_DIR / "it_routes.csv"
    with zipfile.ZipFile(gtfs_zip) as z:
        for member, dest in [("stops.txt", stops_csv), ("routes.txt", routes_csv)]:
            with z.open(member) as src, open(dest, "wb") as out:
                out.write(src.read())

    con.execute(f"""
        INSERT INTO dim_stations
        SELECT
            stop_id AS station_id,
            stop_name AS nom_gare,
            {_normalize_station_name_expr("stop_name")} AS nom_gare_norm,
            NULL AS trigramme,
            stop_id AS code_uic,
            NULL AS code_commune,
            TRY_CAST(stop_lat AS DOUBLE) AS latitude,
            TRY_CAST(stop_lon AS DOUBLE) AS longitude,
            'IT' AS pays
        FROM read_csv('{stops_csv.as_posix()}', header=true, types={{'stop_id': 'VARCHAR'}})
        WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_stop_uic_it AS
        SELECT stop_id, stop_id AS code_uic
        FROM read_csv('{stops_csv.as_posix()}', header=true, types={{'stop_id': 'VARCHAR'}})
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_route_label_it AS
        SELECT route_id, route_short_name AS label
        FROM read_csv('{routes_csv.as_posix()}', header=true, types={{'route_id': 'VARCHAR'}})
        WHERE route_short_name IS NOT NULL AND route_short_name != ''
    """)


def build_fi_reference(con: duckdb.DuckDBPyConnection, stations_json: pathlib.Path = FI_STATIONS_JSON) -> None:
    """Référentiel des gares finlandaises (Digitraffic/VR), pour le temps réel FI.

    Le plus simple des référentiels étrangers après l'Italie : un seul appel JSON (~1000 gares,
    tous types confondus — pas de distinction rail/route ici, Digitraffic ne couvre que le rail).
    `stationUICCode` (identifiant interne finlandais, pas un vrai code UIC international malgré le
    nom) sert de code_uic, trop court pour la regex existante (_STOP_ID_UIC_RE, apps/api/main.py) :
    dim_stop_uic_fi fournit la résolution par identité, comme pour l'Italie.

    `countryCode = 'FI'` : le JSON Digitraffic contient aussi quelques gares frontalières
    étrangères (10 russes, 1 suédoise, vérifié) — sans ce filtre, deux gares distinctes peuvent
    partager un même stationUICCode (ex. 1000 = Ahvenus en Finlande ET Petroskoi/Petrozavodsk en
    Russie), ce qui casserait l'unicité de station_id (cf. dbt/models/marts/dim_stations_multipays.sql).

    Si `stations_json` est absent, ne fait rien (pas d'échec bloquant : ajout, pas un prérequis).
    """
    if not stations_json.exists():
        print(f"[transform] {stations_json} absent, référentiel finlandais ignoré (voir apps/pipeline/ingest.py)")
        return

    con.execute(f"""
        INSERT INTO dim_stations
        SELECT
            CAST(stationUICCode AS VARCHAR) AS station_id,
            stationName AS nom_gare,
            {_normalize_station_name_expr("stationName")} AS nom_gare_norm,
            stationShortCode AS trigramme,
            CAST(stationUICCode AS VARCHAR) AS code_uic,
            NULL AS code_commune,
            latitude,
            longitude,
            'FI' AS pays
        FROM read_json_auto('{stations_json.as_posix()}')
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND countryCode = 'FI'
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_stop_uic_fi AS
        SELECT CAST(stationUICCode AS VARCHAR) AS stop_id, CAST(stationUICCode AS VARCHAR) AS code_uic
        FROM read_json_auto('{stations_json.as_posix()}')
        WHERE countryCode = 'FI'
    """)


def build_pl_reference(con: duckdb.DuckDBPyConnection, gtfs_zip: pathlib.Path = PL_GTFS_ZIP) -> None:
    """Référentiel polonais (gares + résolution stop_sequence -> stop_id), pour le temps réel PL.

    Flux GTFS-RT valide (heures absolues à 100 % testées) mais atypique : les StopTimeUpdate ne
    publient qu'un stop_sequence, pas de stop_id — il faut recouper avec l'horaire théorique
    (stop_times.txt) pour le retrouver. Écrit ce recoupement dans un fichier plat
    (data/raw/pl_stop_sequence_map.csv) lu par apps/streaming/producer.py au démarrage
    (STOP_SEQUENCE_MAP_FILE), plutôt que de le faire à la volée dans l'API — même principe que
    ch_rail_routes.txt pour la Suisse.

    stop_id polonais (ex. "10009_RAIL_1_105") n'est pas un vrai code UIC : dim_stop_uic_pl le
    fournit par identité, comme pour l'Italie/la Finlande. Limite acceptée : plusieurs variantes
    de stop_id (quai, "_FALLBACK"...) pour une même gare physique apparaissent comme des gares
    distinctes dans dim_stations (mêmes coordonnées, pas d'incohérence, juste redondant).

    Si `gtfs_zip` est absent, ne fait rien (pas d'échec bloquant : ajout, pas un prérequis).
    """
    if not gtfs_zip.exists():
        print(f"[transform] {gtfs_zip} absent, référentiel polonais ignoré (voir apps/pipeline/ingest.py)")
        return

    stops_csv = RAW_DIR / "pl_stops.csv"
    stop_times_csv = RAW_DIR / "pl_stop_times.csv"
    with zipfile.ZipFile(gtfs_zip) as z:
        for member, dest in [("stops.txt", stops_csv), ("stop_times.txt", stop_times_csv)]:
            with z.open(member) as src, open(dest, "wb") as out:
                out.write(src.read())

    con.execute(f"""
        INSERT INTO dim_stations
        SELECT
            stop_id AS station_id,
            stop_name AS nom_gare,
            {_normalize_station_name_expr("stop_name")} AS nom_gare_norm,
            NULL AS trigramme,
            stop_id AS code_uic,
            NULL AS code_commune,
            TRY_CAST(stop_lat AS DOUBLE) AS latitude,
            TRY_CAST(stop_lon AS DOUBLE) AS longitude,
            'PL' AS pays
        FROM read_csv('{stops_csv.as_posix()}', header=true, types={{'stop_id': 'VARCHAR'}})
        WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_stop_uic_pl AS
        SELECT stop_id, stop_id AS code_uic
        FROM read_csv('{stops_csv.as_posix()}', header=true, types={{'stop_id': 'VARCHAR'}})
    """)

    # arrival_seconds/departure_seconds : heure théorique en secondes depuis minuit (peut dépasser
    # 86400 pour un trajet après minuit, convention GTFS standard "HH:MM:SS" avec HH >= 24) — sert
    # à calculer le retard nous-mêmes (apps/streaming/producer.py), le flux temps réel polonais ne
    # publiant qu'une heure absolue, jamais de délai (à l'inverse de la Suisse).
    stop_sequence_map_file = RAW_DIR / "pl_stop_sequence_map.csv"
    con.execute(f"""
        COPY (
            SELECT
                trip_id, stop_sequence, stop_id,
                TRY_CAST(split_part(arrival_time, ':', 1) AS INTEGER) * 3600
                    + TRY_CAST(split_part(arrival_time, ':', 2) AS INTEGER) * 60
                    + TRY_CAST(split_part(arrival_time, ':', 3) AS INTEGER) AS arrival_seconds,
                TRY_CAST(split_part(departure_time, ':', 1) AS INTEGER) * 3600
                    + TRY_CAST(split_part(departure_time, ':', 2) AS INTEGER) * 60
                    + TRY_CAST(split_part(departure_time, ':', 3) AS INTEGER) AS departure_seconds
            FROM read_csv('{stop_times_csv.as_posix()}', header=true,
                           types={{'trip_id': 'VARCHAR', 'stop_id': 'VARCHAR'}})
        ) TO '{stop_sequence_map_file.as_posix()}' (HEADER, DELIMITER ',')
    """)
    print(f"[transform] {stop_sequence_map_file} écrit")


def build_de_reference(con: duckdb.DuckDBPyConnection, gtfs_zip: pathlib.Path = DE_GTFS_ZIP) -> None:
    """Référentiel allemand (DELFI, agrégat national gtfs.de), pour le temps réel DE.

    Comme la Suisse : le flux temps réel (realtime-free.pb) mélange tous les modes de transport,
    filtré ici sur route_type='2' (Rail — vocabulaire GTFS de base, pas la variante étendue
    européenne utilisée par la Suisse). Contrairement à la Suisse, heures absolues publiées à
    100 % (vérifié) et stop_id déjà directement présent dans le flux — mais pas de shapes.txt
    (comme la Suisse, contrairement aux Pays-Bas/Pologne) et pas de code UIC séparé : dim_stop_uic_de
    fournit la résolution par identité (stop_id allemand trop court/non numérique pour la regex
    existante, ex. "661713" à 6 chiffres).

    Ne charge ni ne télécharge stop_times.txt (2,17 Go, inutile ici — le filtre rail ne dépend que
    de trips.txt + routes.txt).

    Si `gtfs_zip` est absent, ne fait rien (pas d'échec bloquant : ajout, pas un prérequis).
    """
    if not gtfs_zip.exists():
        print(f"[transform] {gtfs_zip} absent, référentiel allemand ignoré (voir apps/pipeline/ingest.py)")
        return

    stops_csv = RAW_DIR / "de_stops.csv"
    routes_csv = RAW_DIR / "de_routes.csv"
    trips_csv = RAW_DIR / "de_trips.csv"
    with zipfile.ZipFile(gtfs_zip) as z:
        for member, dest in [("stops.txt", stops_csv), ("routes.txt", routes_csv), ("trips.txt", trips_csv)]:
            with z.open(member) as src, open(dest, "wb") as out:
                out.write(src.read())

    con.execute(f"""
        INSERT INTO dim_stations
        SELECT
            stop_id AS station_id,
            stop_name AS nom_gare,
            {_normalize_station_name_expr("stop_name")} AS nom_gare_norm,
            NULL AS trigramme,
            stop_id AS code_uic,
            NULL AS code_commune,
            TRY_CAST(stop_lat AS DOUBLE) AS latitude,
            TRY_CAST(stop_lon AS DOUBLE) AS longitude,
            'DE' AS pays
        FROM read_csv('{stops_csv.as_posix()}', header=true, types={{'stop_id': 'VARCHAR'}})
        WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_stop_uic_de AS
        SELECT stop_id, stop_id AS code_uic
        FROM read_csv('{stops_csv.as_posix()}', header=true, types={{'stop_id': 'VARCHAR'}})
    """)

    # Filtre par trip_id, pas par route_id : le flux temps réel allemand ne publie jamais de
    # route_id (0/88101 testés), contrairement à la Suisse — d'où RAIL_TRIPS_FILE plutôt que
    # RAIL_ROUTES_FILE (cf. apps/streaming/producer.py).
    rail_trip_ids = con.execute(f"""
        SELECT t.trip_id
        FROM read_csv('{trips_csv.as_posix()}', header=true, types={{'route_id': 'VARCHAR', 'trip_id': 'VARCHAR'}}) t
        JOIN read_csv('{routes_csv.as_posix()}', header=true, types={{'route_id': 'VARCHAR', 'route_type': 'VARCHAR'}}) r
          ON r.route_id = t.route_id
        WHERE r.route_type = '{DE_RAIL_ROUTE_TYPE}'
    """).fetchall()
    rail_trips_file = RAW_DIR / "de_rail_trips.txt"
    rail_trips_file.write_text("\n".join(row[0] for row in rail_trip_ids) + "\n", encoding="utf-8")
    print(f"[transform] {rail_trips_file} écrit ({len(rail_trip_ids)} trajets ferroviaires)")


def build_se_reference(con: duckdb.DuckDBPyConnection, gtfs_zip: pathlib.Path = SE_GTFS_ZIP) -> None:
    """Référentiel suédois (GTFS Sweden 3, Trafiklab/Samtrafiken), pour le temps réel SE.

    L'API GTFS-RT Sweden n'expose PAS de flux national SJ (opérateur ferroviaire longue distance,
    absent de l'enum `operator` : https://raw.githubusercontent.com/trafiklab/openApi-docs/master/
    gtfsSwedenRealtime.yaml) — seuls des opérateurs régionaux sont disponibles. producer.py
    interroge celui de Skånetrafiken (cf. se.env), qui couvre entre autres les trains Öresundståg
    (Malmö/Köpenhamn) : le seul, parmi les opérateurs exposés, dont le réseau ferroviaire est
    significatif (vérifié en explorant les autres : essentiellement du bus/tram).

    Comme l'Allemagne (build_de_reference) : le flux mélange tous les modes et ne publie quasiment
    jamais de route_id (844/851 entités testées sur le flux Skånetrafiken) -> filtre par trip_id
    (RAIL_TRIPS_FILE), pas par route_id. route_type au format étendu européen (100-117, comme la
    Suisse, CH_RAIL_ROUTE_TYPES), pas le "2" basique allemand : vérifié sur les lignes Öresundståg
    (802/803/804, route_type=100).

    Contrairement aux autres pays, le zip statique est un agrégat national UNIQUE (tous opérateurs/
    modes confondus, ~58 opérateurs) plutôt qu'un flux déjà propre à un seul opérateur : stops.txt
    couvre donc toute la Suède (182 000 arrêts), inséré tel quel dans dim_stations sans filtre rail
    (même limite acceptée que build_de_reference : /stations exposera aussi des arrêts non
    ferroviaires suédois). stop_id au format NeTEx suédois (16 chiffres, ex.
    "9022050025317002") : trop long pour la regex UIC existante (_STOP_ID_UIC_RE, apps/api/
    main.py) -> dim_stop_uic_se résout par identité, comme l'Italie/la Finlande/la Pologne/
    l'Allemagne.

    Ne charge ni ne télécharge stop_times.txt (693 Mo) ni shapes.txt (2,4 Go), inutiles ici — même
    principe que build_de_reference.

    Si `gtfs_zip` est absent, ne fait rien (pas d'échec bloquant : ajout, pas un prérequis).
    """
    if not gtfs_zip.exists():
        print(f"[transform] {gtfs_zip} absent, référentiel suédois ignoré (voir apps/pipeline/ingest.py)")
        return

    stops_csv = RAW_DIR / "se_stops.csv"
    routes_csv = RAW_DIR / "se_routes.csv"
    trips_csv = RAW_DIR / "se_trips.csv"
    with zipfile.ZipFile(gtfs_zip) as z:
        for member, dest in [("stops.txt", stops_csv), ("routes.txt", routes_csv), ("trips.txt", trips_csv)]:
            with z.open(member) as src, open(dest, "wb") as out:
                out.write(src.read())

    con.execute(f"""
        INSERT INTO dim_stations
        SELECT
            stop_id AS station_id,
            stop_name AS nom_gare,
            {_normalize_station_name_expr("stop_name")} AS nom_gare_norm,
            NULL AS trigramme,
            stop_id AS code_uic,
            NULL AS code_commune,
            TRY_CAST(stop_lat AS DOUBLE) AS latitude,
            TRY_CAST(stop_lon AS DOUBLE) AS longitude,
            'SE' AS pays
        FROM read_csv('{stops_csv.as_posix()}', header=true, types={{'stop_id': 'VARCHAR'}})
        WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_stop_uic_se AS
        SELECT stop_id, stop_id AS code_uic
        FROM read_csv('{stops_csv.as_posix()}', header=true, types={{'stop_id': 'VARCHAR'}})
    """)

    rail_route_types = ",".join(str(t) for t in sorted(CH_RAIL_ROUTE_TYPES))
    rail_trip_ids = con.execute(f"""
        SELECT t.trip_id
        FROM read_csv('{trips_csv.as_posix()}', header=true,
                       types={{'route_id': 'VARCHAR', 'trip_id': 'VARCHAR'}}) t
        JOIN read_csv('{routes_csv.as_posix()}', header=true, types={{'route_id': 'VARCHAR'}}) r
          ON r.route_id = t.route_id
        WHERE TRY_CAST(r.route_type AS INTEGER) IN ({rail_route_types})
    """).fetchall()
    rail_trips_file = RAW_DIR / "se_rail_trips.txt"
    rail_trips_file.write_text("\n".join(row[0] for row in rail_trip_ids) + "\n", encoding="utf-8")
    print(f"[transform] {rail_trips_file} écrit ({len(rail_trip_ids)} trajets ferroviaires)")


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


DIM_STATIONS_MULTIPAYS_CSV = RAW_DIR / "dim_stations_multipays.csv"


def export_dim_stations_multipays(con: duckdb.DuckDBPyConnection, dest: pathlib.Path = DIM_STATIONS_MULTIPAYS_CSV) -> None:
    """Exporte dim_stations (les 8 pays : FR + CH/NL/IT/FI/PL/DE/SE) en CSV, pour remontée dans
    l'entrepôt Snowflake (apps/pipeline/load_cloud.py, dbt/models/marts/dim_stations_multipays.sql)
    — jusqu'ici cette table ne vivait qu'en local (DuckDB), au service de l'API/Bloc 2 uniquement.

    `station_id` seul n'est PAS unique entre pays (ex. "1" existe à la fois en Allemagne et en
    Finlande, cf. apps/api/main.py, _uic_from_stop_id) : la clé Gold composite est construite côté
    dbt (pays || ':' || station_id), pas ici.

    Délimiteur ';' (pas ',') : réutilise le format Snowflake existant SNCF_CSV (infra/terraform/
    snowflake.tf), point-virgule comme les exports data.gouv.fr, plutôt que de définir un nouveau
    FILE FORMAT Snowflake pour ce seul fichier.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (SELECT * FROM dim_stations ORDER BY pays, station_id)
        TO '{dest.as_posix()}' (HEADER, DELIMITER ';')
    """)
    n = con.execute("SELECT COUNT(*) FROM dim_stations").fetchone()[0]
    print(f"[transform] {dest} écrit ({n} gares, 8 pays)")


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
    build_ch_reference(con)
    build_nl_reference(con)
    build_it_reference(con)
    build_fi_reference(con)
    build_pl_reference(con)
    build_de_reference(con)
    build_se_reference(con)
    n_stations, n_fr, n_ch, n_nl, n_it, n_fi, n_pl, n_de, n_se = con.execute(
        "SELECT COUNT(*), SUM(pays = 'FR'), SUM(pays = 'CH'), SUM(pays = 'NL'), SUM(pays = 'IT'), "
        "SUM(pays = 'FI'), SUM(pays = 'PL'), SUM(pays = 'DE'), SUM(pays = 'SE') FROM dim_stations"
    ).fetchone()
    print(f"[transform] table 'dim_stations' construite : {n_stations} gares "
          f"({n_fr} FR, {n_ch} CH, {n_nl} NL, {n_it} IT, {n_fi} FI, {n_pl} PL, {n_de} DE, {n_se} SE)")

    export_dim_stations_multipays(con)

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
