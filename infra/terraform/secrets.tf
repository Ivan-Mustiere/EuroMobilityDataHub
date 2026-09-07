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
