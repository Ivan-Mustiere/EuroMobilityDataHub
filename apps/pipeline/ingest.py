"""Téléchargement des jeux de données SNCF (régularité TGV/TER/Intercités + référentiel gares).

Idempotent : si un fichier existe déjà dans data/raw/, il n'est pas re-téléchargé
sauf si --force est passé.
"""

import argparse
import pathlib

import requests

RAW_DIR = pathlib.Path(__file__).resolve().parent.parent.parent / "data" / "raw"

SOURCES = {
    "regularite_tgv.csv": "https://www.data.gouv.fr/api/1/datasets/r/91fe399d-cafa-4e72-8ba3-56d8717fdad4",
    "regularite_ter.csv": "https://www.data.gouv.fr/api/1/datasets/r/98c86a31-4312-4513-a94d-0bcb9f057f39",
    "regularite_intercites.csv": "https://www.data.gouv.fr/api/1/datasets/r/050a8fe9-6606-4aaf-b77f-b2adccc158aa",
    "gares.csv": "https://www.data.gouv.fr/api/1/datasets/r/cbacca02-6925-4a46-aab6-7194debbb9b7",
    "tarifs_tgv_ouigo.csv": "https://www.data.gouv.fr/api/1/datasets/r/cffcec3b-1c13-4e92-b530-1db00bb3ac1b",
    "tarifs_intercites.csv": "https://www.data.gouv.fr/api/1/datasets/r/929f0d1e-f5b7-4f42-88e1-bf9c6859db73",
}


def download(force: bool = False) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for filename, url in SOURCES.items():
        dest = RAW_DIR / filename
        if dest.exists() and not force:
            print(f"[ingest] {filename} déjà présent, skip (utiliser --force pour retélécharger)")
            continue
        print(f"[ingest] téléchargement de {filename} depuis {url}")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        dest.write_bytes(response.content)
        print(f"[ingest] {filename} écrit ({len(response.content)} octets)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Télécharge les CSV de régularité SNCF (source ODbL, data.gouv.fr)")
    parser.add_argument("--force", action="store_true", help="Re-télécharger même si le fichier existe déjà")
    args = parser.parse_args()
    download(force=args.force)
