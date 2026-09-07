"""Profilage des données brutes via PySpark — cf. Bloc 1, partie 4.1 : "pour les fichiers
horaires volumineux [...] ainsi que pour les flux en temps réel, nous utilisons PySpark".

Version PySpark de `analysis/profile_data.py` (pandas), utilisée spécifiquement par le DAG
Airflow cloud (tâche "validate_and_profile", cf. infra/cloud/airflow/dags/pipeline_dag.py) pour
démontrer l'usage réel de Spark sur le chemin d'ingestion cloud. La version pandas locale reste
inchangée : elle sert les besoins Bloc 2 (chiffres cités dans le dossier), ne pas y toucher.

Usage : python apps/pipeline/profile_spark.py
"""

import pathlib

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
RAW_DIR = ROOT / "data" / "raw"

# outlier_col : colonne numérique sur laquelle chercher des valeurs aberrantes (> 99e percentile).
SOURCES = {
    "regularite_tgv.csv": None,
    "regularite_ter.csv": None,
    "regularite_intercites.csv": None,
    "gares.csv": None,
    "tarifs_tgv_ouigo.csv": "Prix maximum",
    "tarifs_intercites.csv": "Prix maximum",
}


def profile_dataset(spark: SparkSession, path: pathlib.Path, source_name: str, outlier_col: str | None) -> dict:
    df = spark.read.csv(str(path), sep=";", header=True, encoding="UTF-8")
    total_rows = df.count()
    null_counts = {
        col: n
        for col, n in ((c, df.filter(F.col(c).isNull()).count()) for c in df.columns)
        if n > 0
    }
    duplicate_rows = total_rows - df.dropDuplicates().count()

    report = {
        "source": source_name,
        "total_rows": total_rows,
        "total_columns": len(df.columns),
        "null_counts": null_counts,
        "duplicate_rows": duplicate_rows,
    }
    if outlier_col and outlier_col in df.columns:
        values = df.select(F.col(outlier_col).cast("double").alias("v")).na.drop()
        threshold = values.approxQuantile("v", [0.99], 0.01)[0]
        report["outlier_column"] = outlier_col
        report["outliers_99e_percentile"] = values.filter(F.col("v") > threshold).count()
    return report


def run() -> list[dict]:
    spark = SparkSession.builder.appName("euromobilitydatahub-profile").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    reports = []
    try:
        for filename, outlier_col in SOURCES.items():
            path = RAW_DIR / filename
            if not path.exists():
                print(f"[profile_spark] {filename} absent (lancer apps/pipeline/ingest.py) — ignoré")
                continue
            report = profile_dataset(spark, path, filename, outlier_col)
            reports.append(report)
            print(f"[profile_spark] {report}")
    finally:
        spark.stop()
    return reports


if __name__ == "__main__":
    run()
