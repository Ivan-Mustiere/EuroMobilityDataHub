# Rotation automatique du secret RDS (Bloc 1, partie 5.2/b : rotation des secrets tous les 30
# jours). Utilise l'application AWS officielle (Serverless Application Repository) plutôt qu'une
# Lambda maison, pour rester sur du code de rotation maintenu par AWS. Cette Lambda tourne dans le
# VPC (elle doit atteindre RDS en privé) : comme ce VPC n'a pas de NAT Gateway (cf. vpc.tf,
# économie assumée), elle ne peut pas non plus atteindre l'API Secrets Manager par internet — d'où
# le VPC Interface Endpoint dédié ci-dessous (seul coût récurrent réel de ce fichier).

data "aws_partition" "current" {}

resource "aws_security_group" "rds_rotation_lambda" {
  name        = "${local.name_prefix}-rds-rotation-lambda"
  description = "Lambda de rotation Secrets Manager (RDS PostgreSQL) - egress uniquement"
  vpc_id      = aws_vpc.main.id

  # Egress ouvert mais sans portée réelle au-delà du VPC : ce subnet privé n'a pas de route
  # internet (pas de NAT Gateway), donc seuls RDS et le VPC Endpoint ci-dessous sont joignables.
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name_prefix}-rds-rotation-lambda-sg" }
}

resource "aws_security_group" "secretsmanager_endpoint" {
  name        = "${local.name_prefix}-secretsmanager-endpoint"
  description = "VPC Interface Endpoint Secrets Manager - HTTPS depuis le VPC uniquement"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTPS depuis le VPC (Lambda de rotation)"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name_prefix}-secretsmanager-endpoint-sg" }
}

# Un seul subnet (une AZ) : coût minimal assumé pour ce build démo (même logique que le Single-AZ
# de RDS, cf. rds.tf) — la connectivité VPC entre AZ n'est pas affectée par ce choix.
resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.donnees.id]
  security_group_ids  = [aws_security_group.secretsmanager_endpoint.id]
  private_dns_enabled = true

  tags = { Name = "${local.name_prefix}-secretsmanager-endpoint" }
}

data "aws_serverlessapplicationrepository_application" "rds_rotation" {
  application_id = "arn:aws:serverlessrepo:us-east-1:297356227824:applications/SecretsManagerRDSPostgreSQLRotationSingleUser"
}

resource "aws_serverlessapplicationrepository_cloudformation_stack" "rds_rotation" {
  name             = "${local.name_prefix}-rds-rotation"
  application_id   = data.aws_serverlessapplicationrepository_application.rds_rotation.application_id
  semantic_version = data.aws_serverlessapplicationrepository_application.rds_rotation.semantic_version
  capabilities     = data.aws_serverlessapplicationrepository_application.rds_rotation.required_capabilities

  parameters = {
    functionName        = "${local.name_prefix}-rds-rotation"
    endpoint            = "https://secretsmanager.${var.aws_region}.${data.aws_partition.current.dns_suffix}"
    vpcSubnetIds        = aws_subnet.donnees.id
    vpcSecurityGroupIds = aws_security_group.rds_rotation_lambda.id
  }

  depends_on = [aws_vpc_endpoint.secretsmanager]
}

# L'autorisation PostgreSQL pour cette Lambda est un 3e bloc ingress inline sur
# aws_security_group.rds_donnees (vpc.tf), pas une aws_security_group_rule séparée : mélanger les
# deux styles sur un même security group provoque des diffs Terraform incohérents (les blocs
# inline sont gérés comme une liste fermée par le provider AWS).

resource "aws_secretsmanager_secret_rotation" "rds_credentials" {
  secret_id           = aws_secretsmanager_secret.rds_credentials.id
  rotation_lambda_arn = aws_serverlessapplicationrepository_cloudformation_stack.rds_rotation.outputs["RotationLambdaARN"]

  rotation_rules {
    automatically_after_days = 30
  }

  depends_on = [aws_secretsmanager_secret_version.rds_credentials]
}
