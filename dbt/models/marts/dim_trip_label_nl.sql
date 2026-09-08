-- Libellé de ligne (ex. "Intercity", "Sprinter") par trip_id, pour l'affichage sur la carte — la
-- France n'en a pas besoin (nom de gamme déjà déduit du trip_id côté frontend). Filtré sur
-- route_type = 2 (Rail, vocabulaire GTFS de base) : le référentiel néerlandais est multimodal.

select
    cast(t."trip_id" as varchar) as trip_id,
    t."trip_long_name" as label
from {{ source('staging', 'nl_trips') }} t
join {{ source('staging', 'nl_routes') }} r on r."route_id" = t."route_id"
where r."route_type" = 2 and t."trip_long_name" is not null and t."trip_long_name" != ''
