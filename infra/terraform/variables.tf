variable "environment" {
  description = "Environnement déployé (preprod|prod) — chaque valeur correspond à un workspace Terraform distinct (voir infra/README.md, section environnements). \"preprod\" ne suffixe aucun nom de ressource, pour ne pas casser l'infra déjà déployée avant l'introduction de cette variable."
  type        = string
  default     = "preprod"
  validation {
    condition     = contains(["preprod", "prod"], var.environment)
    error_message = "environment doit valoir \"preprod\" ou \"prod\"."
  }
}

variable "aws_region" {
  description = "Région AWS (proche du compte Snowflake choisi pour limiter la latence RDS<->Snowflake)"
  type        = string
  default     = "eu-west-3" # Paris
}

variable "project_name" {
  description = "Préfixe utilisé pour nommer les ressources"
  type        = string
  default     = "euromobilitydatahub"
}

variable "my_ip_cidr" {
  description = "IP publique de l'utilisateur en notation CIDR /32 (ex. \"93.12.45.7/32\"). Seule IP autorisée à atteindre l'API cloud (:8000) et, provisoirement, aucune autre — l'accès admin passe par SSM, pas par une règle entrante."
  type        = string
}

variable "budget_alert_email" {
  description = "Email qui recevra l'alerte de budget AWS"
  type        = string
}

variable "budget_limit_usd" {
  description = "Seuil d'alerte budget mensuel (USD) — dimensionné pour un build d'1 semaine, pas un mois plein"
  type        = number
  default     = 20
}

variable "ec2_instance_type" {
  description = "Type d'instance pour l'hôte Airflow+Kafka — doit être éligible Free Tier sur ce compte AWS (voir infra/README.md)"
  type        = string
  default     = "m7i-flex.large"
}

variable "bastion_instance_type" {
  description = "Type d'instance pour le bastion SSH (zone Administration, cf. bastion.tf) — doit être éligible Free Tier"
  type        = string
  default     = "t3.micro"
}

variable "rds_instance_class" {
  description = "Classe RDS PostgreSQL"
  type        = string
  default     = "db.t3.micro"
}

variable "enable_guardduty" {
  description = "Active GuardDuty. A mettre a false si le compte AWS renvoie SubscriptionRequiredException (compte Free Plan pas encore passe en plan complet)."
  type        = bool
  default     = true
}

variable "snowflake_warehouse_size" {
  description = "Taille du warehouse Snowflake — XS suffit pour une démo (voir infra/README.md)"
  type        = string
  default     = "XSMALL"
}

# Non sensibles (identifiants de compte, pas des secrets) mais nécessaires à Terraform pour
# construire le secret Secrets Manager consommé par l'EC2 (cf. secrets.tf) — le provider
# Snowflake lui-même reste authentifié uniquement par variables d'environnement (providers.tf).
variable "snowflake_organization_name" {
  description = "Identifiant d'organisation Snowflake (visible dans Snowsight, coin bas gauche)"
  type        = string
}

variable "snowflake_account_name" {
  description = "Nom de compte Snowflake (visible dans Snowsight, coin bas gauche)"
  type        = string
}
