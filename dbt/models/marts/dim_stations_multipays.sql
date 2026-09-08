-- Référentiel des gares des 8 pays couverts (FR + CH/NL/IT/FI/PL/DE/SE, Gold) — cf. Bloc 1
-- Tableau 5. Union par pays directement depuis les fichiers GTFS/CSV bruts en Silver (STAGING,
-- extraits par apps/pipeline/ingest.py:extract()) : DuckDB a été éliminé du pipeline, cette
-- harmonisation (dédoublonnage suisse, filtre countryCode finlandais...) vit maintenant ici, en
-- SQL dbt, portée à l'identique depuis les anciennes fonctions build_xx_reference de
-- apps/pipeline/transform.py (supprimé).
--
-- station_id seul n'est PAS unique entre pays (ex. stop_id numérique identique possible dans deux
-- pays, cf. apps/api/main.py _uic_from_stop_id) : id_gare (pays || ':' || station_id) est la vraie
-- clé Gold, testée unique/not_null (schema.yml) — station_id brut est conservé tel quel pour le
-- rapprochement avec le flux temps réel (le stop_id publié par GTFS-RT est toujours une chaîne,
-- d'où le cast(... as varchar) systématique ci-dessous même quand INFER_SCHEMA a détecté un
-- NUMBER côté Silver). Colonnes sources entre guillemets : INFER_SCHEMA crée des identifiants
-- Snowflake sensibles à la casse à partir des en-têtes GTFS/CSV d'origine (minuscules), un
-- identifiant non quoté serait replié en majuscules et ne matcherait plus rien.

with fr as (
    select
        "Id_Gare" as station_id,
        "Nom_Gare" as nom_gare,
        {{ normalize_station_name('"Nom_Gare"') }} as nom_gare_norm,
        "Trigramme" as trigramme,
        "Code_UIC" as code_uic,
        "Code commune" as code_commune,
        try_cast(split_part("Position géographique", ',', 1) as double) as latitude,
        try_cast(trim(split_part("Position géographique", ',', 2)) as double) as longitude,
        'FR' as pays
    from {{ source('staging', 'gares') }}
),

-- Suisse : le flux GTFS-RT ne publie quasiment jamais le code UIC dans le stop_id lui-même
-- (format "sloid") : dédoublonnage par didok (code UIC/DiDok), coordonnées moyennées entre
-- variantes de quai — ANY_VALUE (Snowflake) plutôt que FIRST (DuckDB), même principe.
ch as (
    select
        cast("didok" as varchar) as station_id,
        any_value("stop_name") as nom_gare,
        {{ normalize_station_name('any_value("stop_name")') }} as nom_gare_norm,
        cast(null as varchar) as trigramme,
        cast("didok" as varchar) as code_uic,
        cast(null as varchar) as code_commune,
        cast(avg("stop_lat") as double) as latitude,
        cast(avg("stop_lon") as double) as longitude,
        'CH' as pays
    from {{ source('staging', 'ch_stops') }}
    where "didok" is not null
    group by "didok"
),

nl as (
    select
        cast("stop_id" as varchar) as station_id,
        "stop_name" as nom_gare,
        {{ normalize_station_name('"stop_name"') }} as nom_gare_norm,
        cast(null as varchar) as trigramme,
        cast("stop_id" as varchar) as code_uic,
        cast(null as varchar) as code_commune,
        cast("stop_lat" as double) as latitude,
        cast("stop_lon" as double) as longitude,
        'NL' as pays
    from {{ source('staging', 'nl_stops') }}
    where "stop_lat" is not null and "stop_lon" is not null
),

it as (
    select
        cast("stop_id" as varchar) as station_id,
        "stop_name" as nom_gare,
        {{ normalize_station_name('"stop_name"') }} as nom_gare_norm,
        cast(null as varchar) as trigramme,
        cast("stop_id" as varchar) as code_uic,
        cast(null as varchar) as code_commune,
        cast("stop_lat" as double) as latitude,
        cast("stop_lon" as double) as longitude,
        'IT' as pays
    from {{ source('staging', 'it_stops') }}
    where "stop_lat" is not null and "stop_lon" is not null
),

-- Finlande : le JSON Digitraffic contient aussi des gares frontalières étrangères (russes,
-- suédoises) — countryCode = 'FI' évite une collision de stationUICCode entre pays (bug découvert
-- en ajoutant la Suède cette session : code 1000 = Ahvenus FI ET Petroskoi/Petrozavodsk RU).
fi as (
    select
        cast("stationUICCode" as varchar) as station_id,
        "stationName" as nom_gare,
        {{ normalize_station_name('"stationName"') }} as nom_gare_norm,
        "stationShortCode" as trigramme,
        cast("stationUICCode" as varchar) as code_uic,
        cast(null as varchar) as code_commune,
        cast("latitude" as double) as latitude,
        cast("longitude" as double) as longitude,
        'FI' as pays
    from {{ source('staging', 'fi_stations') }}
    where "countryCode" = 'FI' and "latitude" is not null and "longitude" is not null
),

pl as (
    select
        "stop_id" as station_id,
        "stop_name" as nom_gare,
        {{ normalize_station_name('"stop_name"') }} as nom_gare_norm,
        cast(null as varchar) as trigramme,
        "stop_id" as code_uic,
        cast(null as varchar) as code_commune,
        cast("stop_lat" as double) as latitude,
        cast("stop_lon" as double) as longitude,
        'PL' as pays
    from {{ source('staging', 'pl_stops') }}
    where "stop_lat" is not null and "stop_lon" is not null
),

de as (
    select
        cast("stop_id" as varchar) as station_id,
        "stop_name" as nom_gare,
        {{ normalize_station_name('"stop_name"') }} as nom_gare_norm,
        cast(null as varchar) as trigramme,
        cast("stop_id" as varchar) as code_uic,
        cast(null as varchar) as code_commune,
        cast("stop_lat" as double) as latitude,
        cast("stop_lon" as double) as longitude,
        'DE' as pays
    from {{ source('staging', 'de_stops') }}
    where "stop_lat" is not null and "stop_lon" is not null
),

se as (
    select
        cast("stop_id" as varchar) as station_id,
        "stop_name" as nom_gare,
        {{ normalize_station_name('"stop_name"') }} as nom_gare_norm,
        cast(null as varchar) as trigramme,
        cast("stop_id" as varchar) as code_uic,
        cast(null as varchar) as code_commune,
        cast("stop_lat" as double) as latitude,
        cast("stop_lon" as double) as longitude,
        'SE' as pays
    from {{ source('staging', 'se_stops') }}
    where "stop_lat" is not null and "stop_lon" is not null
),

unioned as (
    select * from fr
    union all select * from ch
    union all select * from nl
    union all select * from it
    union all select * from fi
    union all select * from pl
    union all select * from de
    union all select * from se
)

select
    pays || ':' || station_id as id_gare,
    station_id,
    nom_gare,
    nom_gare_norm,
    trigramme,
    code_uic,
    code_commune,
    latitude,
    longitude,
    pays
from unioned
