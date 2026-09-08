terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    external = {
      source  = "hashicorp/external"
      version = "~> 2.3"
    }
  }

  # Backend local volontaire : projet solo d'une semaine, pas de bootstrap S3/DynamoDB
  # pour le state. Voir infra/README.md pour la procédure destroy en fin de semaine.
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "EuroMobilityDataHub"
      ManagedBy   = "terraform"
      Purpose     = "portfolio-demo-bloc1"
      Environment = var.environment
    }
  }
}

# Authentification par variables d'environnement uniquement (SNOWFLAKE_ORGANIZATION_NAME,
# SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_USER, SNOWFLAKE_AUTHENTICATOR = "SNOWFLAKE_JWT",
# SNOWFLAKE_PRIVATE_KEY[_PASSPHRASE], SNOWFLAKE_ROLE) — jamais dans terraform.tfvars, même
# logique que les credentials AWS (chaîne par défaut du SDK, jamais en dur dans le repo).
# SNOWFLAKE_ROLE doit pouvoir créer database/warehouse/integration (ACCOUNTADMIN pour ce build
# solo — cf. le rôle "admin" du Tableau 13, voir infra/README.md).
#
# snowflake_file_format_csv_resource : encore en preview côté provider au moment du build,
# doit être activé explicitement.
provider "snowflake" {
  preview_features_enabled = ["snowflake_file_format_csv_resource"]
}
