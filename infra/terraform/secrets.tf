# Secret consommé par le pipeline cloud (Airflow sur l'EC2 Applicative, cf. ec2.tf) : jusqu'ici
# le rôle IAM etl_service avait la permission secretsmanager:GetSecretValue (iam.tf) mais aucun
# secret n'existait réellement derrière — ce fichier corrige ce point. Le nom respecte le préfixe
# "${local.name_prefix}/*" déjà autorisé par cette policy, aucun changement IAM nécessaire.

resource "aws_secretsmanager_secret" "etl_cloud_credentials" {
  name        = "${local.name_prefix}/etl-cloud-credentials"
  description = "Credentials Snowflake (utilisateur de service SVC_ETL_LOADER${local.env_suffix_sf}) pour le pipeline cloud"
}

resource "aws_secretsmanager_secret_version" "etl_cloud_credentials" {
  secret_id = aws_secretsmanager_secret.etl_cloud_credentials.id
  secret_string = jsonencode({
    SNOWFLAKE_ORGANIZATION_NAME = var.snowflake_organization_name
    SNOWFLAKE_ACCOUNT_NAME      = var.snowflake_account_name
    SNOWFLAKE_USER              = snowflake_service_user.etl_loader.name
    SNOWFLAKE_ROLE              = snowflake_account_role.etl_loader.name
    SNOWFLAKE_WAREHOUSE         = snowflake_warehouse.main.name
    SNOWFLAKE_DATABASE          = snowflake_database.main.name
    # Une paire de clés RSA dédiée par environnement (voir infra/README.md) : preprod garde le
    # nom de fichier historique (pas de suffixe), prod utilise sa propre clé.
    SNOWFLAKE_PRIVATE_KEY = file(pathexpand("~/.ssh/snowflake_etl_loader_key${local.env_suffix}.p8"))
    BRONZE_BUCKET         = aws_s3_bucket.bronze.bucket
  })
}

# Credentials Snowflake de l'API temps réel (SVC_API, rôle ANALYST — lecture seule MART, cf.
# snowflake.tf) : secret distinct de etl-cloud-credentials ci-dessus (identité de service séparée,
# même principe que etl_loader/analyst déjà séparés côté RBAC).
resource "aws_secretsmanager_secret" "svc_api_credentials" {
  name        = "${local.name_prefix}/svc-api-credentials"
  description = "Credentials Snowflake (utilisateur de service SVC_API${local.env_suffix_sf}) pour l'API temps reel"
}

resource "aws_secretsmanager_secret_version" "svc_api_credentials" {
  secret_id = aws_secretsmanager_secret.svc_api_credentials.id
  secret_string = jsonencode({
    SNOWFLAKE_ORGANIZATION_NAME = var.snowflake_organization_name
    SNOWFLAKE_ACCOUNT_NAME      = var.snowflake_account_name
    SNOWFLAKE_USER              = snowflake_service_user.api.name
    SNOWFLAKE_ROLE              = snowflake_account_role.analyst.name
    SNOWFLAKE_WAREHOUSE         = snowflake_warehouse.main.name
    SNOWFLAKE_DATABASE          = snowflake_database.main.name
    SNOWFLAKE_PRIVATE_KEY       = file(pathexpand("~/.ssh/snowflake_api_key${local.env_suffix}.p8"))
  })
}

# Clé de l'API cloud (apps/api/main.py, require_api_key) : générée par Terraform plutôt que la clé de
# dev en dur du docker-compose.yml racine — nécessaire depuis que l'API est réellement exposée
# publiquement via la DMZ (dmz.tf), pas juste en local.
resource "random_password" "api_key" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "api_key" {
  name        = "${local.name_prefix}/api-key"
  description = "Cle(s) API acceptees par l'API cloud (en-tete X-API-Key, cf. apps/api/main.py)"
}

# CARTO_API_KEY/SNCF_API_KEY/CH_GTFS_SA_TOKEN : tokens tiers optionnels (tuiles de fond de carte,
# cause de retard FR/CH), committés en clair dans infra/cloud/local-test/api.env (même convention
# que les tokens GTFS-RT ci-dessous) — API_KEYS reste généré par Terraform (random_password),
# pas repris du fichier local (qui n'a que "dev-local-key").
data "external" "api_extra_keys_env" {
  program = ["python3", "${path.module}/scripts/parse_env_file.py"]
  query = {
    path = "${path.module}/../../infra/cloud/local-test/api.env"
  }
}

resource "aws_secretsmanager_secret_version" "api_key" {
  secret_id = aws_secretsmanager_secret.api_key.id
  secret_string = jsonencode(merge(
    { for k, v in data.external.api_extra_keys_env.result : k => v if k != "API_KEYS" },
    { API_KEYS = random_password.api_key.result }
  ))
}

# --- Flux GTFS-RT internationaux (CH/DE/NL/IT/FI/PL/SE, cf. apps/streaming/producer.py) ---
# Jusqu'ici ces tokens n'existaient QUE dans infra/cloud/local-test/*.env (committés, cf.
# convention documentée dans ces fichiers), jamais exposés à l'EC2 réel : le bundle applicatif
# exclut délibérément local-test/ (cf. ec2.tf, null_resource.stage_cloud_bundle), pour ne pas
# faire transiter de secrets en clair dans le zip S3. Ces fichiers restent la seule source de
# vérité (data.external, scripts/parse_env_file.py) : pas de duplication des tokens dans ce HCL.
locals {
  gtfs_country_env_files = {
    ch = "ch.env"
    de = "de.env"
    nl = "nl.env"
    it = "it.env"
    fi = "fi.env"
    pl = "pl.env"
    se = "se.env"
  }
}

data "external" "gtfs_country_env" {
  for_each = local.gtfs_country_env_files
  program  = ["python3", "${path.module}/scripts/parse_env_file.py"]
  query = {
    path = "${path.module}/../../infra/cloud/local-test/${each.value}"
  }
}

resource "aws_secretsmanager_secret" "gtfs_country" {
  for_each    = local.gtfs_country_env_files
  name        = "${local.name_prefix}/gtfs-${each.key}"
  description = "Credentials/config du flux GTFS-RT ${upper(each.key)} (producer_${each.key}, cf. apps/streaming/producer.py)"
}

resource "aws_secretsmanager_secret_version" "gtfs_country" {
  for_each      = local.gtfs_country_env_files
  secret_id     = aws_secretsmanager_secret.gtfs_country[each.key].id
  secret_string = jsonencode(data.external.gtfs_country_env[each.key].result)
}

# Clé Trafiklab "GTFS Sweden 3 Static data" (apps/pipeline/ingest.py) : produit API distinct de la
# clé "Realtime" ci-dessus (gtfs-se), consommée par le conteneur airflow (pipeline batch), pas par
# producer_se — secret séparé plutôt qu'un champ de plus dans gtfs-se pour rester 1:1 avec
# infra/cloud/local-test/se_static.env et son usage (cf. docker-compose.yml, service airflow).
data "external" "gtfs_se_static_env" {
  program = ["python3", "${path.module}/scripts/parse_env_file.py"]
  query = {
    path = "${path.module}/../../infra/cloud/local-test/se_static.env"
  }
}

resource "aws_secretsmanager_secret" "gtfs_se_static" {
  name        = "${local.name_prefix}/gtfs-se-static"
  description = "Cle Trafiklab GTFS Sweden 3 Static data (apps/pipeline/ingest.py, build_se_reference)"
}

resource "aws_secretsmanager_secret_version" "gtfs_se_static" {
  secret_id     = aws_secretsmanager_secret.gtfs_se_static.id
  secret_string = jsonencode(data.external.gtfs_se_static_env.result)
}
