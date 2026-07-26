-- Requêtes SQL réelles — remplacent les Requêtes 1 et 2 (exemples fictifs) du dossier.
-- Exécutées sur environments/preprod/db_preprod.duckdb (jeu de données complet, 20 420 lignes).
-- Table source : fact_regularite (voir pipeline/transform.py pour le schéma harmonisé).

-- =============================================================================================
-- Requête 1 — Taux de ponctualité et taux d'annulation moyens par type de ligne
-- (axe "type de ligne" du dossier : grande vitesse / intercité / régional)
-- =============================================================================================
SELECT
    type_ligne,
    COUNT(*)                                   AS nb_mois_liaisons,
    ROUND(AVG(taux_ponctualite), 2)            AS taux_ponctualite_moyen,
    ROUND(AVG(taux_annulation), 2)             AS taux_annulation_moyen,
    ROUND(AVG(retard_moyen_tous_trains_arrivee_min), 2) AS retard_moyen_min  -- NULL hors TGV, cf. limite documentée
FROM fact_regularite
GROUP BY type_ligne
ORDER BY taux_ponctualite_moyen DESC;

-- =============================================================================================
-- Requête 2 — Ponctualité par liaison/région (détail), sur la période complète
-- Filtre un minimum de 12 mois de données pour éviter les axes à faible historique
-- =============================================================================================
SELECT
    type_ligne,
    axe_label,
    COUNT(*)                                   AS nb_mois,
    ROUND(AVG(taux_ponctualite), 2)            AS taux_ponctualite_moyen,
    ROUND(AVG(taux_annulation), 2)             AS taux_annulation_moyen,
    MIN(mois)                                  AS premier_mois,
    MAX(mois)                                  AS dernier_mois
FROM fact_regularite
WHERE taux_ponctualite IS NOT NULL
GROUP BY type_ligne, axe_label
HAVING COUNT(*) >= 12
ORDER BY type_ligne, taux_ponctualite_moyen DESC;

-- =============================================================================================
-- Requête 3 — Top 5 et Bottom 5 liaisons/régions par ponctualité (tous types de ligne confondus)
-- Base de la recommandation partie 5.2 du dossier
-- =============================================================================================
-- premier_mois/dernier_mois sont indispensables ici : certains axes ont cessé de reporter des
-- données il y a plusieurs années (ex. la région "Alsace", fusionnée dans "Grand Est" en 2016) et
-- leur bon classement ne reflète pas la situation actuelle si on ne regarde que la moyenne.
WITH ponctualite_par_axe AS (
    SELECT
        type_ligne,
        axe_label,
        ROUND(AVG(taux_ponctualite), 2) AS taux_ponctualite_moyen,
        COUNT(*)                        AS nb_mois,
        MIN(mois)                       AS premier_mois,
        MAX(mois)                       AS dernier_mois
    FROM fact_regularite
    WHERE taux_ponctualite IS NOT NULL
    GROUP BY type_ligne, axe_label
    HAVING COUNT(*) >= 12
)
SELECT * FROM (
    SELECT *, 'top5' AS classement FROM ponctualite_par_axe ORDER BY taux_ponctualite_moyen DESC LIMIT 5
) UNION ALL (
    SELECT *, 'bottom5' AS classement FROM ponctualite_par_axe ORDER BY taux_ponctualite_moyen ASC LIMIT 5
);

-- =============================================================================================
-- Requête 4 — Évolution mensuelle de la ponctualité par type de ligne (tendance temporelle)
-- =============================================================================================
SELECT
    type_ligne,
    mois,
    ROUND(AVG(taux_ponctualite), 2) AS taux_ponctualite_moyen,
    COUNT(*)                        AS nb_axes
FROM fact_regularite
WHERE taux_ponctualite IS NOT NULL
GROUP BY type_ligne, mois
ORDER BY type_ligne, mois;

-- =============================================================================================
-- Requête 5 — Prix moyen au km (médiane), par type de ligne
-- fact_fares est construite à partir des grilles tarifaires officielles SNCF (ODbL), pas d'un
-- relevé manuel — voir build_fact_fares() dans pipeline/transform.py pour la méthode et sa limite
-- de couverture (rapprochement par nom avec dim_liaisons, partiel).
-- =============================================================================================
SELECT
    type_ligne,
    COUNT(*)                          AS nb_tarifs,
    ROUND(MEDIAN(prix_moyen_km), 3)   AS prix_km_median,
    ROUND(MIN(prix_moyen_km), 3)      AS prix_km_min,
    ROUND(MAX(prix_moyen_km), 3)      AS prix_km_max
FROM fact_fares
WHERE prix_moyen_km IS NOT NULL
  AND classe = '2'
  AND profil_tarifaire = 'Tarif Normal'
GROUP BY type_ligne
ORDER BY type_ligne;

-- =============================================================================================
-- Requête 6 — Détail des liaisons avec prix, distance et prix/km (base de l'analyse Spearman H2)
-- =============================================================================================
SELECT
    type_ligne,
    gare_origine,
    gare_destination,
    distance_km,
    prix_minimum,
    prix_maximum,
    prix_moyen,
    prix_moyen_km
FROM fact_fares
WHERE prix_moyen_km IS NOT NULL
  AND classe = '2'
  AND profil_tarifaire = 'Tarif Normal'
ORDER BY distance_km;
