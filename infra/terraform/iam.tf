# Rôle etl_service (Bloc 1, Table 12) : porté par l'instance profile de l'EC2 Applicative
# (Airflow + Kafka). Écriture S3 Bronze uniquement, lecture Secrets Manager uniquement,
# + SSM pour l'accès admin sans port ouvert (remplace bastion+VPN).
#
# NB rôle "admin" (Table 12) : pas modélisé en tant que rôle IAM Terraform séparé — c'est
# l'utilisateur IAM humain créé manuellement en J0 (accès programmatique + console). Le rôle
# "analyst" (lecture seule Snowflake mart) vit dans le RBAC Snowflake, hors périmètre IAM AWS —
# modélisé via le provider Terraform Snowflake, voir snowflake.tf.

resource "aws_iam_role" "etl_service" {
  name = "${var.project_name}-etl-service"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.etl_service.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "s3_bronze_rw" {
  name = "${var.project_name}-etl-s3-bronze-rw"
  role = aws_iam_role.etl_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.bronze.arn, "${aws_s3_bucket.bronze.arn}/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy" "secrets_read" {
  name = "${var.project_name}-etl-secrets-read"
  role = aws_iam_role.etl_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:*:secret:${var.project_name}/*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "etl_service" {
  name = "${var.project_name}-etl-service"
  role = aws_iam_role.etl_service.name
}

# Rôle snowflake_s3_access (Bloc 1, partie 3.4/b) : assumé par l'utilisateur IAM du compte AWS
# géré par Snowflake (hors de notre organisation) pour lire le bucket Bronze lors des COPY INTO.
# Lecture seule : Snowflake ne fait que consommer S3, jamais y écrire (cf. snowflake.tf).
#
# L'ARN du rôle est recalculé en dur (local.snowflake_s3_role_arn) plutôt que référencé via
# aws_iam_role.snowflake_s3_access.arn, pour casser la dépendance circulaire avec
# snowflake_storage_integration_aws.bronze : Snowflake a besoin de l'ARN du rôle pour créer
# l'intégration, et la trust policy du rôle a besoin de l'iam_user_arn / external_id générés
# (describe_output) par cette intégration. Le format d'un ARN IAM étant déterministe (compte +
# nom), les deux ressources peuvent se référencer sans cycle.
locals {
  snowflake_s3_role_name = "${var.project_name}-snowflake-s3-access"
  snowflake_s3_role_arn  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.snowflake_s3_role_name}"
}

resource "aws_iam_role" "snowflake_s3_access" {
  name = local.snowflake_s3_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = snowflake_storage_integration_aws.bronze.describe_output[0].iam_user_arn }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = {
          "sts:ExternalId" = snowflake_storage_integration_aws.bronze.describe_output[0].external_id
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "snowflake_s3_bronze_read" {
  name = "${var.project_name}-snowflake-s3-bronze-ro"
  role = aws_iam_role.snowflake_s3_access.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "${aws_s3_bucket.bronze.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.bronze.arn
      }
    ]
  })
}
