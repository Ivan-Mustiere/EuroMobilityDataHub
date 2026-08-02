"""Tests statistiques réels (H1 ANOVA, H2 Spearman) sur les données du pipeline.

Usage : python analysis/stats_tests.py [--env dev|preprod|prod]

H1 — ANOVA à un facteur : taux_ponctualite ~ type_ligne
    Adapté par rapport à l'énoncé initial du dossier ("retard ~ type de ligne") : le retard moyen
    en minutes n'est disponible que pour la grande vitesse (voir limite documentée dans
    pipeline/transform.py) — impossible de faire une ANOVA à 3 groupes sur cet indicateur sans
    inventer des valeurs pour TER/Intercités. taux_ponctualite est l'indicateur agrégé disponible
    le plus proche pour les 3 types de ligne, donc utilisé à la place (même logique que
    l'adaptation du P90).
    Chaque liaison/région est d'abord réduite à une seule valeur (moyenne de taux_ponctualite sur
    toute la période disponible, GROUP BY type_ligne, axe_label) avant l'ANOVA : les observations
    mensuelles d'une même liaison ne sont pas indépendantes entre elles (répétitions dans le
    temps), les garder telles quelles violerait l'hypothèse d'indépendance de l'ANOVA. L'ANOVA est
    complétée par l'eta carré (SS_between / SS_total), qui mesure l'ampleur de l'effet au-delà de
    sa seule significativité statistique (cf. conventions de Cohen : 0,01 petit, 0,06 moyen,
    0,14 grand effet).

H2 — Corrélation de Spearman : prix_moyen_km ~ taux_ponctualite_moyen (par liaison)
    Sur les liaisons où fact_fares fournit un prix/km réel (grille tarifaire officielle SNCF,
    filtré 2nde classe / Tarif Normal), croisé avec la ponctualité moyenne réelle de la même
    liaison dans fact_regularite.
"""

import argparse
import os
import pathlib

import duckdb
import yaml
from dotenv import load_dotenv
from scipy import stats

ROOT = pathlib.Path(__file__).resolve().parent.parent
ALPHA = 0.05


def load_config(env: str) -> dict:
    config_path = ROOT / "config" / f"{env}.yaml"
    with open(config_path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def compute_eta_squared(groups: dict) -> float:
    """Ampleur de l'effet (SS_between / SS_total) pour une ANOVA à un facteur."""
    all_values = [v for values in groups.values() for v in values]
    grand_mean = sum(all_values) / len(all_values)
    ss_total = sum((v - grand_mean) ** 2 for v in all_values)
    ss_between = sum(
        len(values) * (sum(values) / len(values) - grand_mean) ** 2
        for values in groups.values()
    )
    if ss_total == 0:
        return 0.0
    return ss_between / ss_total


def compute_h1_anova(con: duckdb.DuckDBPyConnection) -> dict:
    # Une valeur par liaison/région (moyenne sur toute la période) : les observations mensuelles
    # d'une même liaison ne sont pas indépendantes, il faut agréger avant l'ANOVA.
    rows = con.execute("""
        SELECT type_ligne, AVG(taux_ponctualite) AS taux_ponctualite_moyen
        FROM fact_regularite
        WHERE taux_ponctualite IS NOT NULL
        GROUP BY type_ligne, axe_label
    """).fetchall()

    groups = {"grande_vitesse": [], "regional": [], "intercite": []}
    for type_ligne, taux_moyen in rows:
        groups[type_ligne].append(taux_moyen)

    f_stat, p_value = stats.f_oneway(*groups.values())
    return {
        "n_par_groupe": {k: len(v) for k, v in groups.items()},
        "f_stat": f_stat,
        "p_value": p_value,
        "eta2": compute_eta_squared(groups),
        "significatif": p_value < ALPHA,
    }


def compute_h2_spearman(con: duckdb.DuckDBPyConnection) -> dict:
    rows = con.execute("""
        SELECT f.prix_moyen_km, r.taux_ponctualite_moyen
        FROM fact_fares f
        JOIN (
            SELECT type_ligne, axe_label, AVG(taux_ponctualite) AS taux_ponctualite_moyen
            FROM fact_regularite
            WHERE taux_ponctualite IS NOT NULL
            GROUP BY type_ligne, axe_label
        ) r ON r.type_ligne = f.type_ligne AND r.axe_label = f.axe_label_regularite
        WHERE f.prix_moyen_km IS NOT NULL
          AND f.classe = '2'
          AND f.profil_tarifaire = 'Tarif Normal'
    """).fetchall()

    n = len(rows)
    if n < 3:
        return {"n": n, "rho": None, "p_value": None, "significatif": None}

    prix = [r[0] for r in rows]
    ponctualite = [r[1] for r in rows]
    rho, p_value = stats.spearmanr(prix, ponctualite)
    return {"n": n, "rho": rho, "p_value": p_value, "significatif": p_value < ALPHA}


def print_h1(result: dict) -> None:
    print("=== H1 — ANOVA : taux_ponctualite ~ type_ligne (une valeur par liaison/région, moyenne sur la période) ===")
    for type_ligne, n in result["n_par_groupe"].items():
        print(f"  {type_ligne}: n={n} liaisons/régions")
    print(f"  F = {result['f_stat']:.4f}, p-value = {result['p_value']:.6g}, eta2 = {result['eta2']:.4f}")
    if result["significatif"]:
        print(f"  -> p < {ALPHA} : différence significative de ponctualité moyenne selon le type de ligne (H0 rejetée).")
    else:
        print(f"  -> p >= {ALPHA} : pas de différence significative détectée entre les types de ligne (H0 non rejetée).")
    print("  -> eta2 mesure l'ampleur de l'effet (part de la variance expliquée par le type de ligne),")
    print("     au-delà de sa seule significativité statistique.")
    print()
    print("  Note limite : retard_moyen_tous_trains_arrivee_min n'est non-NULL que pour")
    print("  'grande_vitesse' -> une ANOVA à 3 groupes sur le retard en minutes est impossible")
    print("  sans données réelles pour TER/Intercités (voir limite documentée en 4.4/6.2).")
    print()


def print_h2(result: dict) -> None:
    print("=== H2 — Corrélation de Spearman : prix_moyen_km ~ taux_ponctualite ===")
    print(f"  n = {result['n']} liaisons (prix/km réel + ponctualité moyenne réelle)")
    if result["rho"] is None:
        print("  -> Échantillon trop petit pour un test de corrélation fiable. Pas de calcul forcé.")
        return
    print(f"  rho = {result['rho']:.4f}, p-value = {result['p_value']:.6g}")
    if result["significatif"]:
        sens = "positive" if result["rho"] > 0 else "négative"
        print(f"  -> p < {ALPHA} : corrélation {sens} significative entre prix/km et ponctualité (H0 rejetée).")
    else:
        print(f"  -> p >= {ALPHA} : pas de corrélation significative détectée (H0 non rejetée).")
    print()


def run(env: str) -> None:
    config = load_config(env)
    db_path = ROOT / config["database"]["path"]
    con = duckdb.connect(str(db_path), read_only=True)
    print_h1(compute_h1_anova(con))
    print_h2(compute_h2_spearman(con))
    con.close()


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser(description="Exécute les tests statistiques H1 (ANOVA) et H2 (Spearman) sur les données réelles.")
    parser.add_argument("--env", choices=["dev", "preprod", "prod"], default=None,
                         help="Écrase APP_ENV (.env) pour cette exécution")
    args = parser.parse_args()
    run(args.env or os.getenv("APP_ENV", "dev"))
