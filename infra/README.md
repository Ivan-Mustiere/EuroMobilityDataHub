# Infrastructure cloud — EuroMobilityDataHub

Réalise concrètement l'architecture cible décrite au Bloc 1 (S3 Bronze, Snowflake Silver/Gold,
EC2 Airflow+Kafka, RDS PostgreSQL Silver temps réel). Provisionné par Terraform, pensé comme un
build d'une semaine et non une infra permanente — voir "Destroy" en fin de document.

## Vue d'ensemble

```
GTFS-RT SNCF (public, sans clé) --> Kafka (EC2) --> Postgres RDS (fact_realtime, Silver temps réel)
CSV SNCF (data.gouv.fr) --> pipeline/ingest.py --> pipeline/transform.py --> DuckDB local
                                                 --> pipeline/load_cloud.py --> S3 "bronze" --> Snowflake STAGING (Silver) --dbt--> MART (Gold)
Airflow (EC2) orchestre ingest -> transform -> load_cloud, hebdomadaire (cf. cloud/airflow/dags/)
```

| Composant | Où | Fichier(s) |
|---|---|---|
| VPC, subnets, security groups | AWS | `vpc.tf` |
| Bucket S3 "bronze" | AWS | `s3.tf` |
| IAM (etl_service, snowflake_s3_access) | AWS | `iam.tf` |
| CloudTrail, GuardDuty | AWS | `security.tf` |
| Budget guardrail | AWS | `budget.tf` |
| Secrets (Snowflake, RDS) | AWS | `secrets.tf`, `rds.tf` |
| RDS PostgreSQL (Silver temps réel) | AWS | `rds.tf` |
| EC2 Applicative (Kafka + Airflow) | AWS | `ec2.tf` |
| Warehouse, database, rôles RBAC | Snowflake | `snowflake.tf` |
| Kafka, producer/consumer GTFS-RT, DAG Airflow | applicatif (déployé sur l'EC2) | `../cloud/` |

## Environnements (preprod / prod)

Chaque environnement est un **workspace Terraform** distinct (état séparé, mêmes fichiers `.tf`) :
tout nom de ressource dont l'unicité est exigée par AWS ou Snowflake (rôle IAM, secret, base
Snowflake, identifiant RDS...) est calculé via `local.env_suffix`/`local.env_suffix_sf`
(`locals.tf`) — vide pour `preprod` (infra historique, jamais recréée par ce mécanisme), suffixé
`-prod`/`_PROD` sinon. Les deux environnements sont deux stacks **entièrement indépendants**
(VPC, EC2, RDS, warehouse/database Snowflake propres) — rien n'est partagé, à une exception :
le budget AWS (`budget.tf`) et GuardDuty (`security.tf`) sont des ressources uniques par compte,
donc réservées au workspace `preprod` (`count = local.is_preprod ? 1 : 0`).

```bash
# Preprod (déjà déployé, workspace "default")
terraform workspace select default
terraform apply

# Prod (nouveau workspace, stack séparé)
terraform workspace new prod        # une seule fois
terraform workspace select prod
terraform apply -var-file=prod.tfvars
```

Chaque environnement a sa **propre paire de clés RSA** pour l'utilisateur de service Snowflake
(voir prérequis ci-dessous) : `~/.ssh/snowflake_etl_loader_key.p8` pour preprod,
`~/.ssh/snowflake_etl_loader_key-prod.p8` pour prod — jamais partagées, permet une rotation ou
une révocation indépendante par environnement.

## Prérequis avant `terraform apply`

1. **AWS** : credentials via la chaîne par défaut du SDK (`aws configure`, ou variables
   `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`) — jamais en dur dans le repo. Partagées entre
   les deux environnements (même compte AWS).
2. **Snowflake (admin)** : une paire de clés RSA personnelle (voir génération dans l'historique
   de session ou `openssl genrsa`/`pkcs8`), la clé publique enregistrée sur l'utilisateur
   ACCOUNTADMIN (`ALTER USER <user> SET RSA_PUBLIC_KEY='...'`), puis exporter avant `apply` —
   commune aux deux workspaces (même compte Snowflake) :
   ```bash
   export SNOWFLAKE_ORGANIZATION_NAME="..."
   export SNOWFLAKE_ACCOUNT_NAME="..."
   export SNOWFLAKE_USER="..."
   export SNOWFLAKE_AUTHENTICATOR="SNOWFLAKE_JWT"
   export SNOWFLAKE_PRIVATE_KEY="$(cat ~/.ssh/snowflake_key.p8)"
   export SNOWFLAKE_ROLE="ACCOUNTADMIN"   # doit pouvoir créer database/warehouse/integration/user
   ```
3. **Utilisateur de service dédié (par environnement)** : `terraform apply` génère et enregistre
   automatiquement une paire de clés pour `SVC_ETL_LOADER[_PROD]` (cf. `snowflake.tf`) — celle-ci
   doit exister localement AVANT l'apply, à `~/.ssh/snowflake_etl_loader_key[-prod].p8` /
   `.pub.stripped` (le provider Snowflake lit ces fichiers via `file()`/`pathexpand()`) :
   ```bash
   openssl genrsa -out ~/.ssh/snowflake_etl_loader_key-prod 2048
   openssl pkcs8 -topk8 -inform PEM -in ~/.ssh/snowflake_etl_loader_key-prod -outform PEM -nocrypt \
     -out ~/.ssh/snowflake_etl_loader_key-prod.p8
   openssl rsa -in ~/.ssh/snowflake_etl_loader_key-prod -pubout -out ~/.ssh/snowflake_etl_loader_key-prod.pub
   rm ~/.ssh/snowflake_etl_loader_key-prod
   grep -v "PUBLIC KEY" ~/.ssh/snowflake_etl_loader_key-prod.pub | tr -d '\n' \
     > ~/.ssh/snowflake_etl_loader_key-prod.pub.stripped
   ```
4. **`terraform.tfvars` / `prod.tfvars`** (gitignorés, copier les `.example`) : IP publique,
   email d'alerte budget, identifiants de compte Snowflake (non sensibles).

Ordre conceptuel : le garde-fou budgétaire (`budget.tf`) est ce qu'on veut avoir en premier avant
toute ressource facturable — dans ce build, tout est appliqué en un seul `terraform apply` par
workspace (un seul module racine), mais en cas d'apply incrémental (`-target`), commencer par le
budget.

## Dimensionnement et coûts (estimés, région eu-west-3, PAR ENVIRONNEMENT)

| Ressource | Taille | Coût si 24/7 |
|---|---|---|
| EC2 `m7i-flex.large` | 2 vCPU / 8 Go | ~30 $/mois (Free Tier pour la 1ère instance seulement — voir note ci-dessous) |
| RDS `db.t3.micro` | Single-AZ, 20 Go gp3 | ~13 $/mois |
| Snowflake warehouse | XSMALL, auto-suspend 60s | quelques centimes/heure d'utilisation réelle |
| S3, Secrets Manager, CloudTrail | — | < 1 $/mois à ce volume |

**~43 $/mois par environnement si tout tourne en continu**, contre un budget assumé de 20 $/mois
(`budget_limit_usd`, dimensionné pour "un build d'une semaine, pas un mois plein" — cf.
`variables.tf`) — **partagé entre les deux environnements** (un seul budget AWS, cf. ci-dessus),
donc ~86 $/mois potentiels si preprod ET prod tournent en continu en parallèle. Le Free Tier AWS
ne s'applique qu'à hauteur d'un quota mensuel d'heures partagé pour tout le compte, pas par
instance : la seconde instance `m7i-flex.large` (prod) est donc facturée normalement dès que le
quota de la première (preprod) est consommé. Décision explicite prise avec le porteur du projet :
accepter le dépassement plutôt que découper l'infra, le temps de la démo/certification, puis
`destroy` les deux workspaces.

## Zonage réseau : 2 zones sur 4

Le Bloc 1 décrit 4 zones (DMZ / Applicative / Données / Administration). Ce build n'en déploie
que 2 :
- **Applicative** → subnet **public** (pas de NAT Gateway, économie assumée). Héberge l'EC2
  (Kafka, Airflow, API cloud de démo).
- **Données** → subnet **privé**, joignable uniquement depuis le security group Applicative.
  Héberge RDS.
- **DMZ** (API Gateway dédiée) et **Administration** (bastion/VPN) ne sont pas déployées :
  l'API tourne directement sur l'instance Applicative, et l'accès admin passe par AWS Systems
  Manager Session Manager — zéro port entrant, zéro bastion à maintenir.

## Ce qui tourne sur l'EC2

Bootstrap (`templates/ec2_user_data.sh.tftpl`, exécuté une seule fois au premier démarrage) :
installe Docker, récupère `../cloud/` (empaqueté automatiquement par Terraform à chaque apply
via `data.archive_file.cloud_bundle`, jamais d'étape manuelle), récupère les secrets, initialise
le schéma Postgres, puis `docker compose up -d`. Les conteneurs (`restart: unless-stopped`)
survivent à un redémarrage de l'instance sans réexécuter le bootstrap.

Services (`../cloud/docker-compose.yml`) :
- **kafka** (KRaft mono-nœud) + **producer** (poll le flux GTFS-RT public SNCF toutes les 2 min)
  + **consumer** (upsert dans `fact_realtime`, RDS).
- **airflow** (mode `standalone`, léger) : DAG hebdomadaire `euromobilitydatahub_batch`
  (ingest -> transform -> load_cloud -> dbt), cf. `../cloud/airflow/dags/pipeline_dag.py`.
  dbt (`../../dbt/`) promeut STAGING -> MART (2 modèles, 5 tests, cf. `dbt/models/marts/`) ;
  nécessite que les dossiers montés en volume (`data/`, `environments/`, `config/`, `dbt/`) restent
  accessibles en écriture à l'utilisateur non-root du conteneur Airflow (uid 50000) — d'où le
  `chmod 777` sur ces dossiers dans le bootstrap.

Vérifier depuis un poste local (pas de SSH — SSM uniquement) :
```bash
aws ssm start-session --target <instance-id>
# puis, sur l'instance :
docker compose -f /opt/euromobilitydatahub/cloud/docker-compose.yml ps
docker logs -f consumer   # ou producer, airflow, kafka
```

Mettre à jour le code applicatif sur une instance déjà démarrée (après un nouveau
`terraform apply` qui a re-uploadé `_deploy/cloud.zip`) : `docker compose up -d --build` ne suffit
**pas** si seul le contenu d'un volume a changé (pas le `docker-compose.yml` lui-même) — Compose ne
détecte pas de changement et ne recrée pas le conteneur, qui garde alors un point de montage
périmé si le dossier source a été supprimé/recréé entre-temps. Utiliser
`docker compose up -d --force-recreate <service>` dans ce cas.

Tester la stack Kafka/Postgres **en local sans AWS**, avant tout déploiement :
```bash
cd cloud && docker compose --profile local-test up -d --build
```
(utilise `postgis/postgis` en conteneur jetable au lieu de RDS — voir `cloud/local-test/`).

## Limites assumées (dette technique explicite, pas des oublis)

- **Un seul warehouse Snowflake** partagé entre `ETL_LOADER` et `ANALYST` : pas d'isolation des
  coûts par rôle, acceptable pour ce volume de démo.
- **RDS sans sauvegarde** (`backup_retention_period = 0`, `skip_final_snapshot = true`) :
  acceptable, les données sont réalimentées en continu par Kafka et régénérables depuis S3.
- **Kafka mono-nœud, une seule partition** : suffisant pour la démo, pas dimensionné pour un
  vrai débit de production.
- **`_PIP_ADDITIONAL_REQUIREMENTS`** sur le conteneur Airflow (raccourci officiellement documenté
  par Airflow pour du développement) plutôt qu'une image custom — à changer avant tout usage
  prolongé.

## Destroy (fin de build/démo)

Détruire **chaque workspace séparément** — un `terraform destroy` n'agit que sur le workspace
sélectionné :

```bash
cd infra/terraform

terraform workspace select prod
terraform destroy -var-file=prod.tfvars

terraform workspace select default   # preprod
terraform destroy
```

Points de vigilance :
- Le bucket S3 "bronze" a le versioning activé : `terraform destroy` peut échouer s'il contient
  des objets/versions. Vider le bucket avant (`aws s3 rm s3://<bucket> --recursive`) si besoin.
- Les secrets Secrets Manager ont une fenêtre de récupération de 30 jours par défaut : ils
  passent en `pending deletion`, pas supprimés immédiatement (pas de coût significatif entre
  temps, mais le nom reste réservé 30 jours si vous comptez recréer la même infra tout de suite).
- Le rôle IAM `snowflake_s3_access` et la storage integration Snowflake sont liés par un external
  ID généré par Snowflake : les détruire puis les recréer change cet ID automatiquement, aucune
  action manuelle requise.
- Rien côté Snowflake n'est facturé en continu une fois le warehouse suspendu (auto-suspend 60s)
  et les objets STAGING/MART ne prennent que très peu de stockage à ce volume — `terraform
  destroy` reste recommandé en fin de démo mais l'urgence financière y est moindre que pour
  l'EC2/RDS.
