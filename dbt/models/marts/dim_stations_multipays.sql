-- Référentiel des gares des 8 pays couverts (FR + CH/NL/IT/FI/PL/DE/SE, Gold) — cf. Bloc 1
-- Tableau 5. Même contenu que pipeline/transform.py:dim_stations (DuckDB, sert l'API/Bloc 2),
-- exporté en CSV (export_dim_stations_multipays) et chargé ici depuis STAGING (load_cloud.py).
--
-- station_id seul n'est PAS unique entre pays (ex. "1" existe à la fois en Allemagne et en
-- Finlande, cf. apps/api/main.py _uic_from_stop_id) : id_gare (pays || ':' || station_id) est la
-- vraie clé Gold, testée unique/not_null ci-dessous (schema.yml) — station_id brut est conservé
-- tel quel pour le rapprochement avec le flux temps réel.
--
-- CAST (pas TRY_CAST comme dans dim_stations.sql/fact_regularite.sql) pour latitude/longitude :
-- déjà NUMBER après l'inférence de schéma Snowflake (le CSV source contient des décimaux propres,
-- pas une colonne combinée "lat,lon" à parser comme gares.csv) — TRY_CAST n'est valable que depuis
-- VARCHAR/VARIANT côté Snowflake.

select
    "pays" || ':' || "station_id" as id_gare,
    "station_id" as station_id,
    "nom_gare" as nom_gare,
    "nom_gare_norm" as nom_gare_norm,
    "trigramme" as trigramme,
    "code_uic" as code_uic,
    "code_commune" as code_commune,
    cast("latitude" as double) as latitude,
    cast("longitude" as double) as longitude,
    "pays" as pays
from {{ source('staging', 'dim_stations_multipays') }}
