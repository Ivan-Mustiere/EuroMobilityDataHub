-- Table de faits Gold (Bloc 1, Tableau 5) : harmonise TGV/TER/Intercités en un schéma commun.
-- Même logique que pipeline/transform.py:build_harmonized_table, appliquée ici sur les données
-- réellement chargées en STAGING par pipeline/load_cloud.py (COPY INTO).
--
-- Limite documentée (identique à transform.py) : seule la source TGV fournit un retard moyen en
-- minutes ; TER et Intercités ne publient qu'un taux de régularité agrégé.
--
-- CAST (pas TRY_CAST comme dans transform.py) : les colonnes source sont déjà NUMBER, typées par
-- l'inférence de schéma Snowflake (cf. pipeline/load_cloud.py) — TRY_CAST n'est valable que
-- depuis VARCHAR/VARIANT côté Snowflake, un CAST NUMBER->NUMBER ne peut de toute façon pas échouer.

with unioned as (

    select
        "Date" as mois,
        'grande_vitesse' as type_ligne,
        'liaison' as axe_type,
        "Gare de départ" as gare_depart,
        "Gare d'arrivée" as gare_arrivee,
        cast(null as varchar) as region,
        "Gare de départ" || ' -> ' || "Gare d'arrivée" as axe_label,
        cast("Nombre de circulations prévues" as integer) as nb_trains_prevus,
        cast("Nombre de circulations prévues" as integer) - cast("Nombre de trains annulés" as integer) as nb_trains_circules,
        cast("Nombre de trains annulés" as integer) as nb_trains_annules,
        cast("Nombre de trains en retard à l'arrivée" as integer) as nb_trains_retard_arrivee,
        cast("Retard moyen de tous les trains à l'arrivée" as double) as retard_moyen_tous_trains_arrivee_min,
        'regularite_tgv.csv' as source_file
    from {{ source('staging', 'regularite_tgv') }}

    union all

    select
        "Date" as mois,
        'regional' as type_ligne,
        'region' as axe_type,
        cast(null as varchar) as gare_depart,
        cast(null as varchar) as gare_arrivee,
        "Région" as region,
        "Région" as axe_label,
        cast("Nombre de trains programmés" as integer) as nb_trains_prevus,
        cast("Nombre de trains ayant circulé" as integer) as nb_trains_circules,
        cast("Nombre de trains annulés" as integer) as nb_trains_annules,
        cast("Nombre de trains en retard à l'arrivée" as integer) as nb_trains_retard_arrivee,
        cast(null as double) as retard_moyen_tous_trains_arrivee_min,
        'regularite_ter.csv' as source_file
    from {{ source('staging', 'regularite_ter') }}

    union all

    select
        "Date" as mois,
        'intercite' as type_ligne,
        'liaison' as axe_type,
        "Départ" as gare_depart,
        "Arrivée" as gare_arrivee,
        cast(null as varchar) as region,
        "Départ" || ' -> ' || "Arrivée" as axe_label,
        cast("Nombre de trains programmés" as integer) as nb_trains_prevus,
        cast("Nombre de trains ayant circulé" as integer) as nb_trains_circules,
        cast("Nombre de trains annulés" as integer) as nb_trains_annules,
        cast("Nombre de trains en retard à l'arrivée" as integer) as nb_trains_retard_arrivee,
        cast(null as double) as retard_moyen_tous_trains_arrivee_min,
        'regularite_intercites.csv' as source_file
    from {{ source('staging', 'regularite_intercites') }}

)

select
    *,
    case when nb_trains_circules > 0
         then round(100.0 * (nb_trains_circules - nb_trains_retard_arrivee) / nb_trains_circules, 2)
    end as taux_ponctualite,
    case when nb_trains_prevus > 0
         then round(100.0 * nb_trains_annules / nb_trains_prevus, 2)
    end as taux_annulation
from unioned
