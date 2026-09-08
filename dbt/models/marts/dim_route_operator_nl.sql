-- Opérateur (ex. "NS", "Arriva", "Keolis") par route_id — les Pays-Bas ont plusieurs opérateurs
-- régionaux, contrairement à la France (toujours SNCF). Filtré sur route_type = 2 (Rail).

select distinct
    cast(r."route_id" as varchar) as route_id,
    a."agency_name" as operateur
from {{ source('staging', 'nl_routes') }} r
join {{ source('staging', 'nl_agency') }} a on a."agency_id" = r."agency_id"
where r."route_type" = 2
