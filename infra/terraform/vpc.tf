# Zonage réseau conforme au Bloc 1 (4 zones : DMZ / Applicative / Données / Administration),
# toutes déployées (voir infra/README.md pour le détail de chacune) :
#   - "Applicative" -> subnet PUBLIC (pas de NAT Gateway, cf. décision coût du plan).
#     Héberge l'instance EC2 Airflow+Kafka + l'API cloud (plus joignable qu'en interne, cf. DMZ).
#   - "Données"      -> subnet PRIVÉ. Héberge RDS PostgreSQL, joignable uniquement depuis le
#     security group de l'instance Applicative et du bastion (cf. bastion.tf).
#   - "Administration" -> subnet PUBLIC, héberge le bastion SSH (bastion.tf) : accès admin
#     restreint à my_ip_cidr, rebond direct vers RDS (psql). Coexiste avec SSM (toujours actif
#     sur l'instance Applicative) plutôt que de le remplacer — les deux mécanismes d'accès admin
#     du Bloc 1 (bastion et agent managé) sont ainsi tous deux réellement démontrés.
#   - "DMZ" -> pas de subnet dédié (API Gateway est un service managé hors VPC) : voir dmz.tf.
#     Seul point d'entrée public vers l'API, via VPC Link + NLB interne — l'instance Applicative
#     n'est plus joignable directement depuis internet sur le port 8000 (cf. security group
#     ec2_applicative ci-dessous, restreint au CIDR du VPC).

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

# --- Subnet public "Administration" (10.0.1.0/24, cf. Bloc 1 Table 13) : héberge le bastion ---

resource "aws_subnet" "administration" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.project_name}-administration-public" }
}

resource "aws_route_table_association" "administration" {
  subnet_id      = aws_subnet.administration.id
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

# Instance Applicative : AUCUNE règle entrante depuis internet. SSM (agent -> AWS, sortant
# uniquement) gère l'accès admin. Le port de l'API cloud (8000) n'est plus ouvert qu'au VPC lui-
# même : depuis l'introduction de la DMZ (dmz.tf), le seul chemin public vers l'API est
# API Gateway -> VPC Link -> NLB interne -> ce port, jamais directement depuis internet.
resource "aws_security_group" "ec2_applicative" {
  name = "${var.project_name}-ec2-applicative"
  # description inchangée volontairement : cet argument est immuable côté AWS (le modifier force
  # un remplacement complet du security group) — la doc à jour est dans les commentaires ci-dessus.
  description = "Airflow + Kafka + API cloud demo - zero inbound sauf API demo depuis my_ip_cidr"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "API cloud (FastAPI) - uniquement depuis le VPC (NLB de la DMZ, cf. dmz.tf)"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
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

  # Bastion (zone Administration, cf. bastion.tf) : rebond admin direct vers RDS (psql), sans
  # passer par l'instance Applicative.
  ingress {
    description     = "PostgreSQL depuis le bastion (zone Administration)"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.bastion.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-rds-donnees-sg" }
}
