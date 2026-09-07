# Entrepôt Snowflake (Bloc 1, partie 3.4/b-c, Tableau 5) : couches Silver (STAGING) et Gold
# (MART) de l'architecture Medallion. Le chargement Bronze -> Silver se fait par COPY INTO
# depuis le stage externe pointant sur le bucket S3 bronze (cf. s3.tf, iam.tf) ; la promotion
# Silver -> Gold est portée par dbt et reste hors périmètre Terraform (Bloc 1, partie 3.4/c).
#
# NB écart avec l'annexe 17 du Bloc 1 (extrait de DAG) : le snippet illustratif y écrit
# directement `COPY INTO gold.fact_trips`, mais la partie narrative (3.4/b) est sans ambiguïté :
# COPY INTO atterrit dans le schéma staging (Silver), dbt promeut ensuite vers mart (Gold).
# C'est cette version narrative, cohérente avec criteres_evaluation.md, qui est modélisée ici.

resource "snowflake_warehouse" "main" {
  name                = "${upper(var.project_name)}_WH"
  warehouse_size      = var.snowflake_warehouse_size
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  comment             = "Warehouse unique du build (dimensionnement XS, cf. decision cout du plan)"
}

resource "snowflake_database" "main" {
  name    = upper(var.project_name)
  comment = "Entrepot EuroMobilityDataHub (Bloc 1, Tableau 5 - couches Silver/Gold)"
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.main.name
  name     = "STAGING"
  comment  = "Couche Silver analytique : donnees historiques nettoyees, chargees depuis S3 par COPY INTO"
}

resource "snowflake_schema" "mart" {
  database = snowflake_database.main.name
  name     = "MART"
  comment  = "Couche Gold : tables de faits/dimensions construites par dbt depuis STAGING"
}

# --- Intégration de stockage S3 (Bloc 1, partie 3.4/b) ---
# storage_aws_role_arn référence local.snowflake_s3_role_arn (iam.tf) plutôt que la ressource
# aws_iam_role directement : voir le commentaire de ce local pour l'explication de la
# dépendance circulaire évitée entre ce bloc et la trust policy du rôle IAM.
resource "snowflake_storage_integration_aws" "bronze" {
  name                      = "${upper(var.project_name)}_S3_BRONZE_INT"
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = local.snowflake_s3_role_arn
  storage_allowed_locations = ["s3://${aws_s3_bucket.bronze.bucket}/"]
  comment                   = "Delegation d'acces au bucket Bronze pour COPY INTO (role IAM cf. iam.tf)"
}

resource "snowflake_file_format_csv" "bronze_csv" {
  database = snowflake_database.main.name
  schema   = snowflake_schema.staging.name
  name     = "SNCF_CSV"

  field_delimiter              = ","
  skip_header                  = 1
  field_optionally_enclosed_by = "\""
  empty_field_as_null          = true
  null_if                      = ["", "NULL"]
  comment                      = "Format des CSV SNCF deposes dans le bucket Bronze"
}

resource "snowflake_stage_external_s3" "bronze" {
  database            = snowflake_database.main.name
  schema              = snowflake_schema.staging.name
  name                = "BRONZE_STAGE"
  url                 = "s3://${aws_s3_bucket.bronze.bucket}/"
  storage_integration = snowflake_storage_integration_aws.bronze.name
  comment             = "Stage externe utilise par COPY INTO (Airflow SnowflakeOperator, cf. Bloc 1 Annexe 17)"
}

# --- Rôles RBAC (Bloc 1, Tableau 13) ---
# "admin" (lecture+écriture toutes couches) n'est pas modélisé ici : c'est l'utilisateur humain
# ACCOUNTADMIN qui exécute ce `terraform apply` (même logique que le rôle "admin" AWS, cf. iam.tf).

resource "snowflake_account_role" "etl_loader" {
  name    = "ETL_LOADER"
  comment = "Compte de service Airflow (SnowflakeOperator) - extension Snowflake du role etl_service AWS (Tableau 13) : ecriture uniquement sur STAGING"
}

resource "snowflake_account_role" "analyst" {
  name    = "ANALYST"
  comment = "Data analyst / chef de projet (Tableau 13) - lecture seule sur MART, aucun acces a STAGING"
}

resource "snowflake_grant_privileges_to_account_role" "etl_loader_warehouse_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.etl_loader.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.main.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "etl_loader_database_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.etl_loader.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.main.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "etl_loader_staging_schema" {
  privileges        = ["USAGE", "CREATE TABLE"]
  account_role_name = snowflake_account_role.etl_loader.name
  on_schema {
    schema_name = snowflake_schema.staging.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "etl_loader_stage_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.etl_loader.name
  on_schema_object {
    object_type = "STAGE"
    object_name = snowflake_stage_external_s3.bronze.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "etl_loader_file_format_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.etl_loader.name
  on_schema_object {
    object_type = "FILE FORMAT"
    object_name = snowflake_file_format_csv.bronze_csv.fully_qualified_name
  }
}

# ANALYST partage le même warehouse XS que ETL_LOADER (pas d'isolation par warehouse dédié) :
# choix de coût assumé pour ce build d'une semaine, cf. les autres arbitrages de infra/README.md.
resource "snowflake_grant_privileges_to_account_role" "analyst_warehouse_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.analyst.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.main.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_database_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.analyst.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.main.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_mart_schema_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.analyst.name
  on_schema {
    schema_name = snowflake_schema.mart.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_mart_tables_select" {
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.analyst.name
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.mart.fully_qualified_name
    }
  }
}

# Couvre les tables que dbt créera plus tard dans MART, sans révision manuelle des GRANT à
# chaque nouveau modèle dbt.
resource "snowflake_grant_privileges_to_account_role" "analyst_mart_future_tables_select" {
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.analyst.name
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.mart.fully_qualified_name
    }
  }
}
