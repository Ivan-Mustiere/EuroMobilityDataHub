"""Génère les graphiques accessibles à partir des données réelles du pipeline.

Usage : python analysis/charts.py [--env preprod|prod]
Écrit les PNG dans outputs/charts/.

Accessibilité :
- Palette Okabe-Ito (sous-ensemble catégoriel), validée avec le script du skill dataviz
  (`validate_palette.js`) : bande de clarté, plancher de chroma et séparation CVD (daltonisme)
  toutes PASS. Le contraste de l'orange vs fond clair ressort en WARN (2.19:1, sous le seuil de
  3:1) — compensé comme demandé par le script : contours noirs sur toutes les formes, labels de
  valeur directs, et forme/motif de hachures distincts par série (jamais la couleur seule).
- Texte (titres, labels, ticks) en gris quasi noir (#1a1a1a) sur fond blanc : largement > WCAG AA
  4.5:1 (contrainte du projet).
- Heatmap : une seule teinte séquentielle (Blues, clair -> foncé), pas d'arc-en-ciel ; valeurs
  annotées directement dans chaque cellule.
"""

import argparse
import os
import pathlib

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import yaml
from dotenv import load_dotenv

import stats_tests

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "outputs" / "charts"

COLOR_GRANDE_VITESSE = "#0072B2"  # bleu
COLOR_REGIONAL = "#E69F00"        # orange (contraste faible vs fond -> contour noir + label direct)
COLOR_INTERCITE = "#009E73"       # vert bleuté

TYPE_LIGNE_STYLE = {
    "grande_vitesse": {"color": COLOR_GRANDE_VITESSE, "marker": "o", "hatch": None, "label": "Grande vitesse"},
    "regional": {"color": COLOR_REGIONAL, "marker": "s", "hatch": "//", "label": "Régional (TER)"},
    "intercite": {"color": COLOR_INTERCITE, "marker": "^", "hatch": "xx", "label": "Intercités"},
}
ORDER = ["grande_vitesse", "regional", "intercite"]
TEXT_COLOR = "#1a1a1a"

plt.rcParams.update({
    "text.color": TEXT_COLOR,
    "axes.labelcolor": TEXT_COLOR,
    "axes.edgecolor": "#666666",
    "xtick.color": TEXT_COLOR,
    "ytick.color": TEXT_COLOR,
    "axes.titlecolor": TEXT_COLOR,
    "font.size": 11,
})


def load_config(env: str) -> dict:
    with open(ROOT / "config" / f"{env}.yaml", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def get_connection(env: str) -> duckdb.DuckDBPyConnection:
    config = load_config(env)
    db_path = ROOT / config["database"]["path"]
    return duckdb.connect(str(db_path), read_only=True)


def chart_bar_ponctualite_par_type(con: duckdb.DuckDBPyConnection, output_dir: pathlib.Path) -> pathlib.Path:
    """Barres : taux de ponctualité moyen par type de ligne."""
    rows = con.execute("""
        SELECT type_ligne, ROUND(AVG(taux_ponctualite), 1) AS moyenne
        FROM fact_regularite WHERE taux_ponctualite IS NOT NULL
        GROUP BY type_ligne
    """).fetchall()
    data = dict(rows)

    fig, ax = plt.subplots(figsize=(7, 5))
    values = [data[t] for t in ORDER]
    colors = [TYPE_LIGNE_STYLE[t]["color"] for t in ORDER]
    labels = [TYPE_LIGNE_STYLE[t]["label"] for t in ORDER]
    bars = ax.bar(labels, values, color=colors, edgecolor="black", linewidth=1.2)
    for bar, v in zip(bars, values):
        ax.annotate(f"{v:.1f} %", (bar.get_x() + bar.get_width() / 2, v),
                    ha="center", va="bottom", fontsize=11, fontweight="bold", color=TEXT_COLOR)
    ax.set_ylabel("Taux de ponctualité moyen (%)")
    ax.set_title("Ponctualité moyenne par type de ligne (données réelles, jeu complet)")
    ax.set_ylim(0, 100)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    path = output_dir / "barres_ponctualite_par_type.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def chart_histogramme_ponctualite(con: duckdb.DuckDBPyConnection, output_dir: pathlib.Path) -> pathlib.Path:
    """Histogramme : distribution du taux de ponctualité, superposée par type de ligne."""
    fig, ax = plt.subplots(figsize=(8, 5))
    bins = np.linspace(0, 100, 26)
    for t in ORDER:
        rows = con.execute(
            "SELECT taux_ponctualite FROM fact_regularite WHERE type_ligne = ? AND taux_ponctualite IS NOT NULL",
            [t],
        ).fetchall()
        values = [r[0] for r in rows]
        style = TYPE_LIGNE_STYLE[t]
        ax.hist(values, bins=bins, alpha=0.55, color=style["color"], edgecolor="black",
                 linewidth=0.6, hatch=style["hatch"], label=style["label"])
    ax.set_xlabel("Taux de ponctualité (%)")
    ax.set_ylabel("Nombre de (liaison/région × mois)")
    ax.set_title("Distribution du taux de ponctualité par type de ligne (données réelles)")
    ax.legend(frameon=False)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    path = output_dir / "histogramme_ponctualite.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def chart_heatmap_ponctualite_par_annee(con: duckdb.DuckDBPyConnection, output_dir: pathlib.Path) -> pathlib.Path:
    """Heatmap : taux de ponctualité moyen par type de ligne x année (une seule teinte séquentielle)."""
    rows = con.execute("""
        SELECT type_ligne, CAST(SUBSTR(mois, 1, 4) AS INTEGER) AS annee,
               ROUND(AVG(taux_ponctualite), 1) AS moyenne
        FROM fact_regularite
        WHERE taux_ponctualite IS NOT NULL
        GROUP BY type_ligne, annee
        ORDER BY annee
    """).fetchall()

    annees = sorted({r[1] for r in rows})
    lookup = {(t, a): v for t, a, v in rows}
    matrix = np.full((len(ORDER), len(annees)), np.nan)
    for i, t in enumerate(ORDER):
        for j, a in enumerate(annees):
            if (t, a) in lookup:
                matrix[i, j] = lookup[(t, a)]

    vmin, vmax = 50, 100
    fig, ax = plt.subplots(figsize=(12, 4))
    im = ax.imshow(matrix, cmap="Blues", aspect="auto", vmin=vmin, vmax=vmax)
    ax.set_xticks(range(len(annees)))
    ax.set_xticklabels(annees, rotation=45, ha="right")
    ax.set_yticks(range(len(ORDER)))
    ax.set_yticklabels([TYPE_LIGNE_STYLE[t]["label"] for t in ORDER])
    for i in range(len(ORDER)):
        for j in range(len(annees)):
            v = matrix[i, j]
            if not np.isnan(v):
                norm = (v - vmin) / (vmax - vmin)
                text_color = "white" if norm > 0.55 else "black"
                ax.text(j, i, f"{v:.0f}", ha="center", va="center", fontsize=8, color=text_color)
    fig.colorbar(im, ax=ax, label="Taux de ponctualité moyen (%)")
    ax.set_title("Ponctualité moyenne par type de ligne et par année (données réelles)")
    fig.tight_layout()
    path = output_dir / "heatmap_ponctualite_par_annee.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def chart_scatter_prix_ponctualite(con: duckdb.DuckDBPyConnection, output_dir: pathlib.Path) -> pathlib.Path:
    """Nuage de points : prix/km réel vs ponctualité moyenne réelle, par liaison (support visuel H2).

    Régional (TER) absent : agrégé par région, pas de liaison, donc pas de distance/prix au km.
    """
    h2 = stats_tests.compute_h2_spearman(con)
    rows = con.execute("""
        SELECT f.type_ligne, f.prix_moyen_km, r.taux_ponctualite_moyen
        FROM fact_fares f
        JOIN (
            SELECT type_ligne, axe_label, AVG(taux_ponctualite) AS taux_ponctualite_moyen
            FROM fact_regularite WHERE taux_ponctualite IS NOT NULL
            GROUP BY type_ligne, axe_label
        ) r ON r.type_ligne = f.type_ligne AND r.axe_label = f.axe_label_regularite
        WHERE f.prix_moyen_km IS NOT NULL AND f.classe = '2' AND f.profil_tarifaire = 'Tarif Normal'
    """).fetchall()

    fig, ax = plt.subplots(figsize=(7, 6))
    for t in ("grande_vitesse", "intercite"):
        style = TYPE_LIGNE_STYLE[t]
        xs = [r[1] for r in rows if r[0] == t]
        ys = [r[2] for r in rows if r[0] == t]
        ax.scatter(xs, ys, color=style["color"], marker=style["marker"], s=90,
                   edgecolor="black", linewidth=0.8, label=style["label"], alpha=0.85)
    ax.set_xlabel("Prix moyen au km (€/km, grille tarifaire officielle SNCF)")
    ax.set_ylabel("Taux de ponctualité moyen de la liaison (%)")
    if h2["rho"] is not None:
        subtitle = f"n={h2['n']}, rho={h2['rho']:.3f}, p={h2['p_value']:.3g}"
    else:
        subtitle = f"n={h2['n']} (échantillon insuffisant)"
    ax.set_title(f"Prix au km vs ponctualité, par liaison ({subtitle})")
    ax.legend(frameon=False)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    path = output_dir / "nuage_points_prix_ponctualite.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def run(env: str) -> list[pathlib.Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = get_connection(env)
    paths = [
        chart_bar_ponctualite_par_type(con, OUTPUT_DIR),
        chart_histogramme_ponctualite(con, OUTPUT_DIR),
        chart_heatmap_ponctualite_par_annee(con, OUTPUT_DIR),
        chart_scatter_prix_ponctualite(con, OUTPUT_DIR),
    ]
    con.close()
    for path in paths:
        print(f"[charts] {path}")
    return paths


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser(description="Génère les 4 graphiques accessibles à partir des données réelles.")
    parser.add_argument("--env", choices=["preprod", "prod"], default=None,
                         help="Écrase APP_ENV (.env) pour cette exécution")
    args = parser.parse_args()
    run(args.env or os.getenv("APP_ENV", "preprod"))
