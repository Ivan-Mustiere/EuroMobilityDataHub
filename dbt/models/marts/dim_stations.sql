-- Référentiel des gares SNCF (Gold, cf. Bloc 1 Tableau 5 — "Tables de dimensions construites
-- par dbt"). Même logique que pipeline/transform.py:build_dim_stations, appliquée ici sur les
-- données réellement chargées en STAGING (cf. pipeline/load_cloud.py).

select
    "Id_Gare" as station_id,
    "Nom_Gare" as nom_gare,
    {{ normalize_station_name('"Nom_Gare"') }} as nom_gare_norm,
    "Trigramme" as trigramme,
    "Code_UIC" as code_uic,
    "Code commune" as code_commune,
    try_cast(split_part("Position géographique", ',', 1) as double) as latitude,
    try_cast(trim(split_part("Position géographique", ',', 2)) as double) as longitude
from {{ source('staging', 'gares') }}
