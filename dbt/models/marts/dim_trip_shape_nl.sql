-- Tracé réel de la voie (Pays-Bas seulement) : trip_id -> shape_id, pour dessiner le trajet d'un
-- train sélectionné en suivant la géométrie réelle plutôt que des segments droits entre gares.
-- Filtré sur route_type = 2 (Rail) : le référentiel néerlandais est multimodal.
--
-- Limite acceptée (héritée de apps/pipeline/transform.py build_nl_reference) : ne couvre que les
-- trajets dont le trip_id temps réel correspond exactement à celui du GTFS statique (~91% des
-- trains observés à l'exploration) — le reste garde le tracé en ligne droite (cf. apps/api/main.py).

select
    cast(t."trip_id" as varchar) as trip_id,
    cast(t."shape_id" as varchar) as shape_id
from {{ source('staging', 'nl_trips') }} t
join {{ source('staging', 'nl_routes') }} r on r."route_id" = t."route_id"
where r."route_type" = 2 and t."shape_id" is not null
