"""Téléchargement des jeux de données SNCF (régularité TGV/TER/Intercités + référentiel gares)
et des référentiels statiques GTFS suisse, néerlandais et italien (gares, pour le temps réel
CH/NL/IT).

Idempotent : si un fichier existe déjà dans data/raw/, il n'est pas re-téléchargé
sauf si --force est passé.
"""

import argparse
import os
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
    # Référentiel du flux temps réel Trenitalia France (trains transfrontaliers Paris/Lyon-Milan,
    # cf. apps/streaming/producer.py) : minuscule (~20 gares), pas de rotation d'URL datée.
    "it_gtfs_static.zip": "https://thello.axelor.com/public/gtfs/gtfs.zip",
}

# Horaire théorique suisse (opentransportdata.swiss) : sert uniquement à résoudre les gares
# (stops.txt) et le type de ligne (routes.txt) pour le temps réel CH (cf. apps/api/main.py,
# apps/streaming/producer.py) — pas de régularité mensuelle suisse pour l'instant (aucun dataset
# agrégé équivalent aux CSV SNCF ci-dessus n'a été trouvé, cf. plan Suisse).
# Le nom de fichier change à chaque publication : on résout l'URL du jour via l'API CKAN
# d'opendata.swiss plutôt que de coder une date en dur.
CH_GTFS_CKAN_PACKAGE = "https://opendata.swiss/api/3/action/package_show?id=fahrplan-2026-gtfs2020"
CH_GTFS_ZIP_FILENAME = "ch_gtfs_static.zip"

# Horaire théorique néerlandais (OVapi, agrégat national ND-OV/NDOVloket) : sert à résoudre les
# gares pour le temps réel NL — même principe que la Suisse ci-dessus, mais plus simple (pas de
# rotation d'URL datée, pas d'authentification requise). Contrairement au flux GTFS-RT de la
# France/Suisse, `trainUpdates.pb` (utilisé par producer.py) publie déjà une heure absolue par
# arrêt : pas besoin de stop_times.txt pour reconstituer un horaire théorique.
NL_GTFS_ZIP_URL = "https://gtfs.ovapi.nl/nl/gtfs-nl.zip"
NL_GTFS_ZIP_FILENAME = "nl_gtfs_static.zip"

# Référentiel des gares finlandaises (Digitraffic/VR), pour le temps réel FI — cf.
# apps/streaming/producer_fi.py. Minuscule (~15 Ko, toutes les gares en un seul appel JSON), pas
# de bundle GTFS à télécharger. Exige `Accept-Encoding: gzip` (406 sinon).
FI_STATIONS_URL = "https://rata.digitraffic.fi/api/v1/metadata/stations"
FI_STATIONS_FILENAME = "fi_stations.json"

# Référentiel polonais (republication communautaire des données officielles PKP/PLK, qui exigent
# normalement une clé sur dane.plk-sa.pl) : gares + horaire théorique complet (stop_times.txt),
# nécessaire pour résoudre les arrêts du flux temps réel PL (qui ne publie qu'un stop_sequence,
# cf. apps/pipeline/transform.py build_pl_reference).
PL_GTFS_ZIP_URL = "https://mkuran.pl/gtfs/polish_trains.zip"
PL_GTFS_ZIP_FILENAME = "pl_gtfs_static.zip"

# Référentiel allemand (DELFI, agrégat national gtfs.de) : sert à isoler les trains (route_type=2)
# du flux temps réel réel-free.pb (apps/streaming/producer.py, RAIL_ROUTES_FILE), qui mélange tous
# les modes de transport — même principe que la Suisse. 284 Mo (dominé par stop_times.txt, non
# utilisé ici), mais stable (pas de rotation d'URL datée).
DE_GTFS_ZIP_URL = "https://download.gtfs.de/germany/free/latest.zip"
DE_GTFS_ZIP_FILENAME = "de_gtfs_static.zip"

# Référentiel suédois (GTFS Sweden 3, Trafiklab/Samtrafiken) : agrégat national unique (tous
# opérateurs/modes confondus, contrairement aux sources ci-dessus qui sont déjà spécifiques à un
# pays/opérateur), sert à isoler les trains du flux temps réel SE (route_type, cf.
# apps/pipeline/transform.py build_se_reference). Contrairement à toutes les autres sources,
# nécessite une clé API (SE_GTFS_STATIC_KEY, palier Bronze Trafiklab, 60 requêtes/30j — d'où le
# comportement idempotent ci-dessous, encore plus important qu'ailleurs) passée en query param
# `key`, pas en en-tête.
SE_GTFS_ZIP_URL_TEMPLATE = "https://opendata.samtrafiken.se/gtfs-sweden/sweden.zip?key={key}"
SE_GTFS_ZIP_FILENAME = "se_gtfs_static.zip"


def _resolve_ch_gtfs_zip_url() -> str:
    response = requests.get(CH_GTFS_CKAN_PACKAGE, timeout=30)
    response.raise_for_status()
    resources = response.json()["result"]["resources"]
    zip_resources = [r for r in resources if r.get("format") == "ZIP"]
    if not zip_resources:
        raise RuntimeError(f"Aucune ressource ZIP trouvée dans le catalogue CKAN {CH_GTFS_CKAN_PACKAGE}")
    # Les ressources sont listées de la plus récente à la plus ancienne.
    return zip_resources[0]["url"]


def _fetch(filename: str, url: str, timeout: int, headers: dict | None = None, display_url: str | None = None) -> None:
    print(f"[ingest] téléchargement de {filename} depuis {display_url or url}")
    response = requests.get(url, headers=headers or {}, timeout=timeout)
    response.raise_for_status()
    (RAW_DIR / filename).write_bytes(response.content)
    print(f"[ingest] {filename} écrit ({len(response.content)} octets)")


def download(force: bool = False) -> None:
    """Isole les échecs par source (Bloc 1) : une API en panne ne doit pas empêcher les autres
    sources de se télécharger — chaque échec est loggé et collecté ci-dessous plutôt que de
    stopper immédiatement toute la fonction, mais la tâche Airflow échoue quand même à la fin
    (alerte SES, cf. pipeline_dag.py) si au moins une source a échoué : rien n'est masqué."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []

    def _try(filename: str, url: str, timeout: int, headers: dict | None = None, display_url: str | None = None) -> None:
        dest = RAW_DIR / filename
        if dest.exists() and not force:
            print(f"[ingest] {filename} déjà présent, skip (utiliser --force pour retélécharger)")
            return
        try:
            _fetch(filename, url, timeout, headers, display_url)
        except Exception as exc:
            print(f"[ingest] ÉCHEC {filename} : {exc!r}")
            errors.append(filename)

    for filename, url in SOURCES.items():
        _try(filename, url, timeout=60)

    if (RAW_DIR / CH_GTFS_ZIP_FILENAME).exists() and not force:
        print(f"[ingest] {CH_GTFS_ZIP_FILENAME} déjà présent, skip (utiliser --force pour retélécharger)")
    else:
        try:
            ch_url = _resolve_ch_gtfs_zip_url()
        except Exception as exc:
            print(f"[ingest] ÉCHEC {CH_GTFS_ZIP_FILENAME} (résolution CKAN) : {exc!r}")
            errors.append(CH_GTFS_ZIP_FILENAME)
        else:
            _try(CH_GTFS_ZIP_FILENAME, ch_url, timeout=120)

    _try(NL_GTFS_ZIP_FILENAME, NL_GTFS_ZIP_URL, timeout=120)
    _try(FI_STATIONS_FILENAME, FI_STATIONS_URL, timeout=30, headers={"Accept-Encoding": "gzip"})
    _try(PL_GTFS_ZIP_FILENAME, PL_GTFS_ZIP_URL, timeout=120)
    _try(DE_GTFS_ZIP_FILENAME, DE_GTFS_ZIP_URL, timeout=240)

    se_key = os.environ.get("SE_GTFS_STATIC_KEY")
    if not se_key:
        print("[ingest] SE_GTFS_STATIC_KEY absent, référentiel suédois ignoré (voir infra/cloud/local-test/se_static.env)")
    else:
        _try(
            SE_GTFS_ZIP_FILENAME,
            SE_GTFS_ZIP_URL_TEMPLATE.format(key=se_key),
            timeout=240,
            display_url=SE_GTFS_ZIP_URL_TEMPLATE.format(key="***"),
        )

    if errors:
        raise RuntimeError(f"{len(errors)} source(s) en échec (voir logs ci-dessus) : {', '.join(errors)}")


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()  # SE_GTFS_STATIC_KEY en local hors docker (déjà dans l'environnement du conteneur airflow sinon)

    parser = argparse.ArgumentParser(description="Télécharge les CSV de régularité SNCF (source ODbL, data.gouv.fr) et le référentiel GTFS suisse")
    parser.add_argument("--force", action="store_true", help="Re-télécharger même si le fichier existe déjà")
    args = parser.parse_args()
    download(force=args.force)
