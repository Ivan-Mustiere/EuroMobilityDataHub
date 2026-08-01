"""Profilage de la qualité des données brutes (pandas) — cf. Bloc 1, partie 4.2.b et Annexe 11.

Étape de profilage effectuée AVANT transformation, directement sur les CSV bruts de data/raw/ :
détecte les valeurs manquantes, les doublons et les valeurs aberrantes. Reprend la structure de
`profile_dataset()` décrite en Annexe 11 du Bloc 1 (implémentation pandas directe ; ydata-profiling
n'apporterait rien de plus sur ce volume de données).

Usage : python analysis/profile_data.py
"""

import pathlib

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"

# outlier_col : colonne numérique sur laquelle chercher des valeurs aberrantes (> 99e percentile).
# None si la source n'a pas de colonne numérique pertinente pour ce contrôle.
SOURCES = {
    "regularite_tgv.csv": None,
    "regularite_ter.csv": None,
    "regularite_intercites.csv": None,
    "gares.csv": None,
    "tarifs_tgv_ouigo.csv": "Prix maximum",
    "tarifs_intercites.csv": "Prix maximum",
}


def profile_dataset(df: pd.DataFrame, source_name: str, outlier_col: str | None = None) -> dict:
    """Reprend la structure de profile_dataset() du Bloc 1 (Annexe 11) : source, total_rows,
    null_counts, duplicate_rows, + un contrôle d'outliers si une colonne prix est fournie.
    """
    report = {
        "source": source_name,
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "null_counts": {col: int(n) for col, n in df.isnull().sum().items() if n > 0},
        "duplicate_rows": int(df.duplicated().sum()),
    }
    if outlier_col and outlier_col in df.columns:
        values = pd.to_numeric(df[outlier_col], errors="coerce")
        threshold = values.quantile(0.99)
        report["outlier_column"] = outlier_col
        report["outliers_99e_percentile"] = int((values > threshold).sum())
    return report


def run() -> list[dict]:
    reports = []
    for filename, outlier_col in SOURCES.items():
        path = RAW_DIR / filename
        if not path.exists():
            print(f"[profile] {filename} absent (lancer pipeline/ingest.py) — ignoré")
            continue
        df = pd.read_csv(path, sep=";", encoding="utf-8-sig")
        report = profile_dataset(df, filename, outlier_col=outlier_col)
        reports.append(report)
        print(f"[profile] {report}")
    return reports


if __name__ == "__main__":
    run()
