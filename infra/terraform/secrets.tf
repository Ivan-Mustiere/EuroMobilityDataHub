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

# Clé de l'API cloud (api/main.py, require_api_key) : générée par Terraform plutôt que la clé de
# dev en dur du docker-compose.yml racine — nécessaire depuis que l'API est réellement exposée
# publiquement via la DMZ (dmz.tf), pas juste en local.
resource "random_password" "api_key" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "api_key" {
  name        = "${local.name_prefix}/api-key"
  description = "Cle(s) API acceptees par l'API cloud (en-tete X-API-Key, cf. api/main.py)"
}

resource "aws_secretsmanager_secret_version" "api_key" {
  secret_id = aws_secretsmanager_secret.api_key.id
  secret_string = jsonencode({
    API_KEYS = random_password.api_key.result
  })
}
