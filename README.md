# EuroMobilityDataHub

Plateforme de centralisation de données ferroviaires européennes. Ce dépôt contient le pipeline
de données réel (Python + DuckDB) qui produit un baromètre comparatif de la ponctualité et des
tarifs ferroviaires à partir de vraies données ouvertes SNCF, ainsi que la documentation
d'architecture du projet (`docs/`).

## Workflow Git

Branche par défaut : **`preprod`** (base de toutes les PR). Branche **`prod`** protégée,
réservée aux versions validées (résultats définitifs du dossier).

```
feature/xxx --PR--> preprod --PR--> prod
```

Les deux branches exigent une PR + le check CI (`build-and-smoke-test`). Pas de push direct.

## Workflow Git

Branche par défaut : **`preprod`** (base de toutes les PR). Branche **`prod`** protégée,
réservée aux versions validées (résultats définitifs du dossier).

```
feature/xxx --PR--> preprod --PR--> prod
```

Les deux branches exigent une PR + le check CI (`build-and-smoke-test`). Pas de push direct.

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
./.venv/bin/python pipeline/ingest.py            # télécharge les CSV dans data/raw/
./.venv/bin/python pipeline/transform.py --env prod   # harmonise + charge dans environments/prod/db_prod.duckdb

# Tests statistiques (ANOVA H1, Spearman H2) sur les données de l'environnement
./.venv/bin/python analysis/stats_tests.py --env preprod

# Profilage qualité (pandas) des CSV bruts : valeurs manquantes, doublons, outliers
./.venv/bin/python analysis/profile_data.py

# Graphiques accessibles (barres, histogramme, heatmap, nuage de points) -> outputs/charts/
./.venv/bin/python analysis/charts.py --env preprod

# Tableau de résultats consolidé -> outputs/resultats.csv
./.venv/bin/python analysis/export_results.py --env preprod
```

## Tests

```bash
./.venv/bin/pytest tests/ -v
```

Tests unitaires sur `pipeline/transform.py` (harmonisation du schéma des 3 sources, calcul de
`taux_ponctualite`/`taux_annulation`, gestion des cas à 0 train programmé/circulé, sélection de
l'échantillon dev) à partir de fixtures CSV réduites dans `tests/fixtures/`. Exécutés en CI à
chaque push/PR sur `preprod`/`prod`, en plus du smoke test end-to-end du pipeline complet.

## API

API REST (FastAPI) qui sert en lecture les données produites par le pipeline (référentiel des
gares, ponctualité par type de ligne/liaison/mois) — reflète les tables réelles de la couche Gold
décrite dans le Bloc 1 (`dim_stations`, `fact_regularite`), pas de données factices.

```bash
API_KEYS=dev-local-key APP_ENV=preprod ./.venv/bin/uvicorn api.main:app --reload --no-access-log
```

RGPD (aligné sur le Bloc 1, partie 5.1/d) : les access logs bruts d'uvicorn (IP en clair) sont
désactivés (`--no-access-log`) et remplacés par un log applicatif qui anonymise systématiquement
l'IP (deux derniers octets masqués) avant écriture — voir `anonymize_ip()` dans `api/main.py`.

Documentation interactive : http://127.0.0.1:8000/docs

Contrôle d'accès (Bloc 1, Tableau 13 — rôle `api_consumer`) : `/stations*` et `/regularite*`
exigent l'en-tête `X-API-Key` (401 sinon) et sont limités à 30 requêtes/minute par clé (429
au-delà). `/health` et `/metrics` restent ouverts, nécessaires à la supervision (Prometheus,
sondes). Clé(s) valides définies par la variable `API_KEYS` (voir `.env.example`).

| Endpoint | Auth | Description |
|---|---|---|
| `GET /health` | non | Statut + environnement actif |
| `GET /stations` | oui | Référentiel des gares (`q` = filtre nom, `limit`) |
| `GET /stations/{station_id}` | oui | Détail d'une gare |
| `GET /regularite` | oui | Ponctualité/retard (filtres `type_ligne`, `mois`, `axe_label`, `limit`) |
| `GET /regularite/stats` | oui | Moyennes par type de ligne |
| `GET /metrics` | non | Métriques Prometheus (requêtes, latence par endpoint) |

Chiffrement en transit (C1.4.2) : `docker compose up api caddy` démarre en plus un reverse-proxy
Caddy qui termine le TLS avec sa CA interne (pas de nom de domaine requis) sur
https://localhost:8443 — le navigateur avertit sur le certificat (CA locale, attendu). Voir
`caddy/Caddyfile`.

## Monitoring (Prometheus + Grafana)

```bash
docker compose up api prometheus grafana
```

- Prometheus scrape `GET /metrics` de l'API toutes les 10s (`monitoring/prometheus.yml`)
- Grafana sur http://localhost:3030 (admin/admin) — datasource et dashboard provisionnés
  automatiquement (`monitoring/grafana/provisioning/`) : requêtes/s par endpoint, latence p95,
  total requêtes, erreurs, répartition par statut HTTP

## Utilisation (Docker)

Un service Compose par environnement (`dev`, `preprod`, `prod`), même image, seule la variable
`APP_ENV` change. Les dossiers `data/`, `environments/`, `config/`, `outputs/` sont montés en
volumes, donc persistés sur l'hôte entre deux runs.

```bash
docker compose run --rm dev
docker compose run --rm preprod --skip-download
docker compose run --rm prod
docker compose up api          # sert l'API sur http://localhost:8000 (APP_ENV=preprod par défaut, ajustable via API_ENV)
```

Renseigner `HOST_UID`/`HOST_GID` dans `.env` (valeurs par défaut : `id -u`/`id -g`) pour que les
fichiers écrits dans les volumes t'appartiennent plutôt qu'à `root`.

Ne jamais écrire les résultats du dossier depuis **dev** : toujours repasser par **preprod** puis
**prod**.

## Pipeline cloud (Bloc 1, partie 3.4)

`pipeline/load_cloud.py` réalise le flux Bronze -> Silver réel : upload des CSV bruts vers le
bucket S3 (partitionné opérateur/type/date), puis `COPY INTO` Snowflake STAGING avec inférence de
schéma automatique (aucune colonne codée en dur). Nécessite l'infra Terraform déployée
(`infra/terraform/`, voir `infra/README.md`) et les credentials Snowflake du rôle `ETL_LOADER` :

```bash
BRONZE_BUCKET=<sortie bronze_bucket_name> \
SNOWFLAKE_ORGANIZATION_NAME=... SNOWFLAKE_ACCOUNT_NAME=... SNOWFLAKE_USER=SVC_ETL_LOADER \
SNOWFLAKE_ROLE=ETL_LOADER SNOWFLAKE_PRIVATE_KEY="$(cat ~/.ssh/snowflake_etl_loader_key.p8)" \
python pipeline/load_cloud.py
```

Sur l'infra cloud (EC2 Applicative), ce flux est orchestré par un DAG Airflow hebdomadaire
(`cloud/airflow/dags/pipeline_dag.py`) ; en parallèle, un producer/consumer Kafka (`cloud/`)
ingère en continu le flux GTFS-RT public de la SNCF vers RDS PostgreSQL (`fact_realtime`, couche
Silver temps réel du Bloc 1). Détails, dimensionnement et procédure destroy : `infra/README.md`.

## Structure du projet

```
docs/                   documentation d'architecture du projet
infra/terraform/        infrastructure AWS + Snowflake (Terraform), voir infra/README.md
cloud/                  stack déployée sur l'EC2 : Kafka, producer/consumer GTFS-RT, DAG Airflow
pipeline/               ingest.py, transform.py, run.py, load_cloud.py (Bronze -> Snowflake)
api/                    API REST FastAPI (main.py) servant les données du pipeline
monitoring/             config Prometheus + provisioning Grafana (datasource, dashboard)
tests/                  tests unitaires (pytest) sur pipeline/transform.py et api/main.py
analysis/               requêtes SQL, profilage qualité, tests statistiques, graphiques, export des résultats
config/                 dev.yaml / preprod.yaml / prod.yaml
environments/           bases DuckDB par environnement (non versionnées)
data/raw/               CSV téléchargés depuis data.gouv.fr (non versionnés, régénérables via ingest.py)
data/processed/         données nettoyées (non versionné)
outputs/                résultats/graphiques/stats définitifs (issus de l'environnement prod)
```

## Données sources

Toutes sous licence **ODbL** (data.gouv.fr) :
- Régularité mensuelle SNCF : TGV, TER, Intercités
- Référentiel des gares (coordonnées GPS)
- Grilles tarifaires : TGV INOUI/OUIGO, Intercités (prix minimum/maximum par trajet/classe)

Limites connues :
- Granularité TER par région (pas par liaison comme TGV/Intercités) ; retard moyen en minutes
  disponible uniquement pour le TGV ; P90 par train non calculable (pas de donnée individuelle).
- Le rapprochement par nom entre gares et libellés de liaison est partiel (~41 % des liaisons
  obtiennent une distance/un prix au km) — gares étrangères, libellés combinés ("ALBI/RODEZ"),
  variantes de nom trop éloignées ne sont volontairement pas rapprochées (pas de fuzzy matching).
