# EuroMobilityDataHub — Bloc 2 (RNCP 39586)

Pipeline de données réel (Python + DuckDB) produisant les résultats du dossier de certification
Bloc 2 : un baromètre comparatif de la ponctualité et des tarifs ferroviaires, à partir de vraies
données ouvertes SNCF.

## Prérequis

- Python 3.12+, ou Docker + Docker Compose

## Installation (venv local)

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp .env.example .env   # ajuster APP_ENV si besoin (dev par défaut)
```

## Variables d'environnement (`.env`)

| Variable | Rôle | Défaut |
|---|---|---|
| `APP_ENV` | Environnement utilisé quand `--env` n'est pas passé en ligne de commande (`dev`, `preprod`, `prod`) | `dev` |
| `HOST_UID` / `HOST_GID` | Utilisés par Docker Compose pour que les fichiers écrits dans les volumes t'appartiennent (valeurs : `id -u` / `id -g`) | `1000` |

Le reste de la configuration (chemin de la base DuckDB, taille d'échantillon, niveau de log) est
propre à chaque environnement et vit dans `config/dev.yaml`, `config/preprod.yaml`,
`config/prod.yaml`.

## Utilisation (venv local)

```bash
# Pipeline complet (téléchargement des CSV si absents + transformation) pour un environnement
./.venv/bin/python pipeline/run.py --env dev

# Sans --env : utilise APP_ENV défini dans .env
./.venv/bin/python pipeline/run.py

# Sauter le téléchargement (CSV déjà présents dans data/raw/)
./.venv/bin/python pipeline/run.py --env preprod --skip-download

# Étapes séparées
./.venv/bin/python pipeline/ingest.py            # télécharge les 3 CSV dans data/raw/
./.venv/bin/python pipeline/transform.py --env prod   # harmonise + charge dans environments/prod/db_prod.duckdb
```

## Utilisation (Docker)

Un service Compose par environnement (`dev`, `preprod`, `prod`), même image, seule la variable
`APP_ENV` change. Les dossiers `data/`, `environments/`, `config/`, `tarifs/`, `outputs/` sont
montés en volumes, donc persistés sur l'hôte entre deux runs.

```bash
docker compose run --rm dev
docker compose run --rm preprod --skip-download
docker compose run --rm prod
```

Renseigner `HOST_UID`/`HOST_GID` dans `.env` (valeurs par défaut : `id -u`/`id -g`) pour que les
fichiers écrits dans les volumes t'appartiennent plutôt qu'à `root`.

Ne jamais écrire les résultats du dossier depuis **dev** : toujours repasser par **preprod** puis
**prod**.

## Structure du projet

```
data/raw/              CSV téléchargés depuis data.gouv.fr (non versionnés, régénérables via ingest.py)
data/processed/         données nettoyées (non versionné)
pipeline/               ingest.py, transform.py, run.py
analysis/               requêtes SQL, tests statistiques, graphiques (à venir — Jour 2+)
tarifs/                 échantillon de prix collecté manuellement (à venir — Jour 3)
config/                 dev.yaml / preprod.yaml / prod.yaml
environments/           bases DuckDB par environnement (non versionnées)
outputs/                résultats/graphiques/stats définitifs (issus de l'environnement prod)
dossier/                dossier de certification mis à jour
```

## Données sources

3 jeux de données régularité mensuelle SNCF (licence **ODbL**, data.gouv.fr) : TGV, TER,
Intercités.

Limites connues : la granularité TER est par région (et non par liaison comme TGV/Intercités), le
retard moyen en minutes n'est disponible que pour le TGV, et le P90 par train n'est pas calculable
(pas de donnée de retard individuel dans les 3 sources).
