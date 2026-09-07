# Zonage réseau simplifié par rapport au Bloc 1 (4 zones : DMZ / Applicative / Données /
# Administration). Ce build n'en déploie que 2, pour un motif documenté (voir infra/README.md) :
#   - "Applicative" -> devient un subnet PUBLIC (pas de NAT Gateway, cf. décision coût du plan).
#     Héberge l'instance EC2 Airflow+Kafka + l'API cloud de démo.
#   - "Données"      -> reste un subnet PRIVÉ. Héberge RDS PostgreSQL, joignable uniquement
#     depuis le security group de l'instance Applicative.
#   - "DMZ" (API Gateway dédiée) et "Administration" (bastion+VPN) ne sont pas déployées :
#     l'API de démo tourne directement sur l'instance Applicative, et l'accès admin passe par
#     AWS Systems Manager Session Manager (aucun bastion/VPN nécessaire, zéro port entrant).

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${var.project_name}-vpc" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.project_name}-igw" }
}

# --- Subnet public "Applicative" (10.0.2.0/24, cf. Bloc 1 Table 13) ---

resource "aws_subnet" "applicative" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.project_name}-applicative-public" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "${var.project_name}-public-rt" }
}

resource "aws_route_table_association" "applicative" {
  subnet_id      = aws_subnet.applicative.id
  route_table_id = aws_route_table.public.id
}

# --- Subnet privé "Données" (10.0.3.0/24, cf. Bloc 1 Table 13) ---

resource "aws_subnet" "donnees" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.3.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]
  tags              = { Name = "${var.project_name}-donnees-private" }
}

# Second subnet privé requis par RDS (DB subnet group exige >= 2 AZ) même en Single-AZ.
resource "aws_subnet" "donnees_secondary" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.4.0/24"
  availability_zone = data.aws_availability_zones.available.names[1]
  tags              = { Name = "${var.project_name}-donnees-private-2" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.project_name}-private-rt" }
}

resource "aws_route_table_association" "donnees" {
  subnet_id      = aws_subnet.donnees.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "donnees_secondary" {
  subnet_id      = aws_subnet.donnees_secondary.id
  route_table_id = aws_route_table.private.id
}

# --- S3 Gateway Endpoint (gratuit) : accès S3 depuis le VPC sans passer par Internet/NAT.
# Rend réel le point du dossier "accès S3 via VPC Endpoint privé" (Bloc 1, partie 5.2/b).

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id, aws_route_table.private.id]

  tags = { Name = "${var.project_name}-s3-endpoint" }
}

# --- Security Groups ---

# Instance Applicative : AUCUNE règle entrante par défaut. SSM (agent -> AWS, sortant
# uniquement) gère l'accès admin. Seule exception : le port de démo de l'API cloud, restreint
# à l'IP publique de l'utilisateur.
resource "aws_security_group" "ec2_applicative" {
  name        = "${var.project_name}-ec2-applicative"
  description = "Airflow + Kafka + API cloud demo - zero inbound sauf API demo depuis my_ip_cidr"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "API cloud de demo (FastAPI, APP_ENV=cloud)"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
  }

  egress {
    description = "Sortant libre (pulls Docker, GTFS-RT, S3, Secrets Manager, Snowflake, SSM)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-ec2-applicative-sg" }
}

# RDS PostgreSQL : accessible uniquement depuis le SG de l'instance Applicative (intra-VPC).
resource "aws_security_group" "rds_donnees" {
  name        = "${var.project_name}-rds-donnees"
  description = "PostgreSQL - accessible uniquement depuis ec2_applicative"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "PostgreSQL depuis instance Applicative uniquement"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.ec2_applicative.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-rds-donnees-sg" }
}
