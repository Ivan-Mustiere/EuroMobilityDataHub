# Infrastructure cloud — EuroMobilityDataHub

Réalise concrètement l'architecture cible décrite au Bloc 1 (S3 Bronze, Snowflake Silver/Gold,
EC2 Airflow+Kafka, RDS PostgreSQL Silver temps réel). Provisionné par Terraform, pensé comme un
build d'une semaine et non une infra permanente — voir "Destroy" en fin de document.

## Vue d'ensemble

```
GTFS-RT SNCF (public, sans clé) --> Kafka (EC2) --> Postgres RDS (fact_realtime, Silver temps réel)
CSV SNCF (data.gouv.fr) --> apps/pipeline/ingest.py --> apps/pipeline/transform.py --> DuckDB local
                                                 --> apps/pipeline/load_cloud.py --> S3 "bronze" --> Snowflake STAGING (Silver) --dbt--> MART (Gold)
Airflow (EC2) orchestre ingest -> transform -> load_cloud, hebdomadaire (cf. infra/cloud/airflow/dags/)
Internet --> API Gateway (DMZ) --> VPC Link --> NLB interne --> EC2 Applicative:8000 (api)
```

| Composant | Où | Fichier(s) |
|---|---|---|
| VPC, subnets, security groups | AWS | `vpc.tf` |
| Bucket S3 "bronze" | AWS | `s3.tf` |
| IAM (etl_service, snowflake_s3_access) | AWS | `iam.tf` |
| CloudTrail, GuardDuty | AWS | `security.tf` |
| Budget guardrail | AWS | `budget.tf` |
| Secrets (Snowflake, RDS, clé API) | AWS | `secrets.tf`, `rds.tf` |
| RDS PostgreSQL (Silver temps réel) | AWS | `rds.tf` |
| Rotation automatique du secret RDS (30 jours) | AWS | `secrets_rotation.tf` |
| EC2 Applicative (Kafka + Airflow + API) | AWS | `ec2.tf` |
| Bastion SSH (zone Administration) | AWS | `bastion.tf` |
| API Gateway + VPC Link + NLB (zone DMZ) | AWS | `dmz.tf` |
| Warehouse, database, rôles RBAC | Snowflake | `snowflake.tf` |
| Kafka, DAG Airflow, docker-compose de déploiement | applicatif (déployé sur l'EC2) | `cloud/` |
| Producer/consumer Kafka GTFS-RT | applicatif (déployé sur l'EC2) | `../apps/streaming/` |

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
5. **Clé SSH du bastion (par environnement)** : générée hors Terraform, jamais dans le state
   (`bastion.tf` ne lit que la clé publique) :
   ```bash
   ssh-keygen -t ed25519 -f ~/.ssh/euromobilitydatahub_bastion_key[-prod] -N "" \
     -C "euromobilitydatahub-bastion"
   ```

Ordre conceptuel : le garde-fou budgétaire (`budget.tf`) est ce qu'on veut avoir en premier avant
toute ressource facturable — dans ce build, tout est appliqué en un seul `terraform apply` par
workspace (un seul module racine), mais en cas d'apply incrémental (`-target`), commencer par le
budget.

## Dimensionnement et coûts (estimés, région eu-west-3, PAR ENVIRONNEMENT)

| Ressource | Taille | Coût si 24/7 |
|---|---|---|
| EC2 `m7i-flex.large` | 2 vCPU / 8 Go | ~30 $/mois (Free Tier pour la 1ère instance seulement — voir note ci-dessous) |
| EC2 `t3.micro` (bastion) | 2 vCPU / 1 Go | ~7 $/mois |
| RDS `db.t3.micro` | Single-AZ, 20 Go gp3 | ~13 $/mois |
| NLB interne (DMZ) | 1 nœud | ~17 $/mois (tarif horaire fixe, quel que soit le trafic) |
| API Gateway (HTTP API) | — | ~1 $/million de requêtes — négligeable à ce volume |
| Snowflake warehouse | XSMALL, auto-suspend 60s | quelques centimes/heure d'utilisation réelle |
| S3, Secrets Manager, CloudTrail | — | < 1 $/mois à ce volume |

**~68 $/mois par environnement si tout tourne en continu**, contre un budget assumé de 20 $/mois
(`budget_limit_usd`, dimensionné pour "un build d'une semaine, pas un mois plein" — cf.
`variables.tf`) — **partagé entre les deux environnements** (un seul budget AWS, cf. ci-dessus),
donc ~136 $/mois potentiels si preprod ET prod tournent en continu en parallèle. Le Free Tier AWS
ne s'applique qu'à hauteur d'un quota mensuel d'heures partagé pour tout le compte, pas par
instance : la seconde instance `m7i-flex.large` (prod) est donc facturée normalement dès que le
quota de la première (preprod) est consommé — et le NLB n'a de toute façon aucun volet Free Tier,
quel que soit l'environnement. Décision explicite prise avec le porteur du projet : accepter le
dépassement plutôt que découper l'infra, le temps de la démo/certification, puis `destroy` les
deux workspaces.

## Zonage réseau : 4 zones sur 4

Le Bloc 1 décrit 4 zones (DMZ / Applicative / Données / Administration) avec des CIDR précis
(Tableau 14), toutes déployées avec ces CIDR à une exception documentée près :

| Zone | CIDR | Composants | Accès autorisé |
|---|---|---|---|
| DMZ (publique) | `10.0.1.0/24` | NLB interne (`dmz.tf`) | Internet → DMZ uniquement |
| Applicative | `10.0.2.0/24` | EC2 : Airflow, Kafka, API (`ec2.tf`) | DMZ → Applicative |
| Données | `10.0.3.0/24` | RDS, VPC Endpoint S3 | Applicative → Données |
| Administration | `10.0.6.0/24` *(voir écart ci-dessous)* | Bastion SSH (`bastion.tf`) | VPN admin uniquement |

**Écart CIDR documenté** : le Tableau 14 place Administration sur `10.0.4.0/24`. En pratique, ce
CIDR est occupé par un second subnet privé technique (`donnees_secondary`, exigé par RDS pour son
DB subnet group, ≥ 2 AZ même en Single-AZ). Constaté en conditions réelles : l'instance RDS
tourne dans l'AZ de ce subnet, et AWS refuse de libérer son ENI pour permettre un changement de
CIDR (`ModifyDBSubnetGroup` échoue avec *"subnets to be deleted are currently in use"*, même avec
`create_before_destroy`) — le libérer exigerait de déplacer l'instance RDS de production vers une
autre AZ (Multi-AZ temporaire + failover, ou snapshot/restore), un risque jugé disproportionné
pour une base contenant des données réelles et irremplaçables (`fact_realtime`, alimentée en
continu par le consumer Kafka). Administration a donc été placée sur `10.0.6.0/24`, un CIDR libre,
plutôt que de risquer la base — cf. commentaire détaillé en tête de `vpc.tf`.

Détail par zone :
- **Applicative** → subnet public (pas de NAT Gateway, économie assumée). Héberge l'EC2 (Kafka,
  Airflow, service API) — **plus joignable directement depuis internet** sur le port 8000 depuis
  l'introduction de la DMZ (le security group `ec2_applicative` ne l'autorise plus que depuis le
  CIDR du VPC, cf. `vpc.tf`).
- **Données** → subnet privé, joignable depuis le security group Applicative ET depuis le bastion
  (ci-dessous). Héberge RDS, avec **SSL obligatoire** (`rds.force_ssl = 1` via
  `aws_db_parameter_group`, cf. `rds.tf`) : une connexion `sslmode=disable` est rejetée par
  Postgres (*"no pg_hba.conf entry ..., no encryption"*), seul `sslmode=require`/`prefer` passe.
- **Administration** → subnet public (`10.0.6.0/24`), héberge un **bastion SSH derrière un VPN
  WireGuard** (`bastion.tf`) : le security group n'ouvre que le port UDP/51820 (WireGuard) depuis
  `my_ip_cidr` — **aucune règle sur le port 22**, SSH n'écoute que sur l'interface du tunnel
  (`10.99.0.1`, cf. `templates/bastion_user_data.sh.tftpl`). Rebond direct vers RDS via `psql` une
  fois dans le tunnel. Clés SSH et WireGuard générées hors Terraform (jamais dans le state) :
  `~/.ssh/euromobilitydatahub_bastion_key[-prod]` et `~/.wireguard/{server,client}_*.key[-prod]`.
  Coexiste avec **SSM** (toujours actif sur l'instance Applicative) plutôt que de le remplacer —
  les deux mécanismes d'accès admin du Bloc 1 (VPN+bastion et agent managé) sont ainsi réellement
  démontrés côte à côte, pas juste l'un ou l'autre.
- **DMZ** (`10.0.1.0/24`, `dmz.tf`) → héberge le NLB interne (sans IP publique). L'API Gateway
  elle-même (service managé AWS, toujours hors VPC) n'est pas déplaçable dans un subnet. Seul
  point d'entrée public vers l'API : **API Gateway (HTTP API) → VPC Link → NLB (subnet DMZ) → EC2
  Applicative:8000**. Clé API réelle générée par Terraform et stockée dans Secrets Manager
  (`secrets.tf`, `api_key`), plus la clé de démo en dur du docker-compose racine.

Se connecter au bastion (VPN WireGuard puis SSH, cf. config client à générer à partir de
`~/.wireguard/client_private.key[-prod]` + `~/.wireguard/server_public.key[-prod]` +
`bastion_public_ip:51820`, `AllowedIPs = 10.99.0.1/32`) :
```bash
sudo wg-quick up ./wg0-client.conf
ssh -i ~/.ssh/euromobilitydatahub_bastion_key ubuntu@10.99.0.1
```

## Rotation automatique du secret RDS

Le mot de passe RDS tourne automatiquement tous les 30 jours (`secrets_rotation.tf`), via l'app
AWS officielle du Serverless Application Repository (`SecretsManagerRDSPostgreSQLRotationSingleUser`)
plutôt qu'une Lambda maison. Cette Lambda tourne dans le VPC (subnet Données) et atteint l'API
Secrets Manager via un **VPC Interface Endpoint** dédié — ce VPC n'a pas de NAT Gateway (économie
assumée), donc pas d'autre chemin sortant depuis un subnet privé.

Point important : le secret change de format pour respecter le schéma standard attendu par cette
Lambda (`host`/`port`/`dbname`/`username`/`password`, plus `engine`), différent des clés
`PG*`/majuscules historiques. `consumer.py` (seul composant qui parle à RDS) relit ce secret via
`boto3` **à chaque reconnexion** plutôt que de garder un mot de passe figé au démarrage du
conteneur — indispensable pour survivre à une rotation sans redémarrage manuel (vérifié en
conditions réelles : la première activation de la rotation change le mot de passe immédiatement,
et le consumer a continué à écrire dans `fact_realtime` sans interruption ni intervention).
`aws_db_instance.donnees` et `aws_secretsmanager_secret_version.rds_credentials` ignorent les
dérives sur le mot de passe (`lifecycle.ignore_changes`) pour ne jamais écraser un mot de passe
tourné avec l'ancien `random_password.rds_master` figé dans le state Terraform.

Appeler l'API via la DMZ (seul chemin qui fonctionne désormais) :
```bash
API_KEY=$(aws secretsmanager get-secret-value --secret-id euromobilitydatahub/api-key \
  --query SecretString --output text | jq -r .API_KEYS)
curl -H "X-API-Key: $API_KEY" "$(terraform output -raw api_gateway_url)stations?limit=5"
```

## Ce qui tourne sur l'EC2

Bootstrap (`templates/ec2_user_data.sh.tftpl`, exécuté une seule fois au premier démarrage) :
installe Docker, récupère un bundle miroir exact du dépôt (`cloud/` = `infra/cloud/`,
`../apps/pipeline/`, `../apps/api/`, `../apps/streaming/`, `../Dockerfile`,
`../requirements.txt` — empaquetés automatiquement par Terraform à chaque apply via
`data.archive_file.cloud_bundle`, jamais d'étape manuelle), récupère les secrets, initialise le
schéma Postgres, puis `docker compose up -d`. Les conteneurs (`restart: unless-stopped`)
survivent à un redémarrage de l'instance sans réexécuter le bootstrap.

Services (`cloud/docker-compose.yml`) :
- **api** : l'API FastAPI (`../apps/api/main.py`), seule cible du NLB de la DMZ — plus reçue
  directement depuis internet (cf. "Zonage réseau" plus haut). Lit `environments/preprod/`
  (le DAG écrit toujours dans cet environnement, quel que soit l'environnement AWS).
- **kafka** (KRaft mono-nœud) + **producer** (poll le flux GTFS-RT public SNCF toutes les 2 min)
  + **consumer** (upsert dans `fact_realtime`, RDS) — cf. `../apps/streaming/`.
- **airflow** (mode `standalone`, léger) : DAG hebdomadaire `euromobilitydatahub_batch`
  (ingest -> transform -> load_cloud -> dbt), cf. `cloud/airflow/dags/pipeline_dag.py`.
  dbt (`../dbt/`) promeut STAGING -> MART (2 modèles, 5 tests, cf. `dbt/models/marts/`) ;
  nécessite que les dossiers montés en volume (`data/`, `environments/`, `config/`, `dbt/`) restent
  accessibles en écriture à l'utilisateur non-root du conteneur Airflow (uid 50000) — d'où le
  `chmod 777` sur ces dossiers dans le bootstrap.

Vérifier depuis un poste local (pas de SSH — SSM uniquement) :
```bash
aws ssm start-session --target <instance-id>
# puis, sur l'instance :
docker compose -f /opt/euromobilitydatahub/infra/cloud/docker-compose.yml ps
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
cd infra/cloud && docker compose --profile local-test up -d --build
```
(utilise `postgis/postgis` en conteneur jetable au lieu de RDS — voir `infra/cloud/local-test/`).

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
