-- Points du tracé réel des voies néerlandaises, UN ROW PAR POINT (format déjà natif de
-- shapes.txt GTFS) — contrairement à la version DuckDB historique qui agrégeait en LIST par
-- shape_id, on garde le format long ici : apps/api/main.py (_nl_shape_for_trip) ne demande jamais
-- qu'UN SEUL trajet à la fois (WHERE shape_id = ?, ORDER BY shape_pt_sequence), jamais la table
-- entière — même principe que le fix mémoire de cette session (ne jamais précharger tous les
-- tracés en RAM, 6,5M points au total).

select
    cast("shape_id" as varchar) as shape_id,
    "shape_pt_sequence" as shape_pt_sequence,
    cast("shape_pt_lat" as double) as latitude,
    cast("shape_pt_lon" as double) as longitude
from {{ source('staging', 'nl_shapes') }}
