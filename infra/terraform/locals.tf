# Séparation prod/preprod (voir infra/README.md, section environnements) : chaque environnement
# est un workspace Terraform distinct (état séparé), appliqué avec sa propre valeur de
# var.environment. Le suffixe est VIDE pour "preprod" — c'est l'infra déjà déployée avant
# l'introduction de cette variable ; la garder sans suffixe évite de la recréer (les identifiants
# AWS/Snowflake globalement uniques comme un nom de rôle IAM ou de base Snowflake forcent un
# remplacement s'ils changent).
locals {
  is_preprod    = var.environment == "preprod"
  env_suffix    = local.is_preprod ? "" : "-${var.environment}"        # ex. "-prod", pour les noms AWS (kebab-case)
  env_suffix_sf = local.is_preprod ? "" : "_${upper(var.environment)}" # ex. "_PROD", pour les objets Snowflake
  name_prefix   = "${var.project_name}${local.env_suffix}"
}
