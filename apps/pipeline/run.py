"""Point d'entrée du pipeline : ingestion puis transformation, pour un environnement donné.

Usage : python apps/pipeline/run.py [--env dev|preprod|prod]
Si --env est omis, la valeur vient de APP_ENV dans .env (défaut : dev).
"""

import argparse
import os

from dotenv import load_dotenv

from ingest import download
from transform import run as transform_run

if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(description="Exécute le pipeline complet (ingest + transform) pour un environnement.")
    parser.add_argument("--env", choices=["dev", "preprod", "prod"], default=None,
                         help="Écrase APP_ENV (.env) pour cette exécution")
    parser.add_argument("--skip-download", action="store_true", help="Ne pas re-vérifier/télécharger les CSV bruts")
    args = parser.parse_args()

    env = args.env or os.getenv("APP_ENV", "dev")

    if not args.skip_download:
        download()

    transform_run(env)
