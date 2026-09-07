# Zonage réseau conforme au Bloc 1, Tableau 14 (4 zones) :
#   Zone              CIDR          Composants                    Accès autorisé
#   DMZ (publique)    10.0.1.0/24   NLB de la DMZ (dmz.tf)         Internet -> DMZ uniquement
#   Applicative       10.0.2.0/24   Airflow, Kafka, API (ec2.tf)   DMZ -> Applicative
#   Données           10.0.3.0/24   RDS, S3 VPC Endpoint           Applicative -> Données
#   Administration    10.0.6.0/24   Bastion SSH (bastion.tf)       VPN admin uniquement
#
# Note : "DMZ" au sens Bloc 1 désigne le sous-réseau qui héberge le Load Balancer interne, pas
# l'API Gateway elle-même (service managé AWS, toujours hors VPC quel que soit le fournisseur
# cloud — non déplaçable dans un sous-réseau). Voir dmz.tf.
#
# Écart documenté vs Tableau 14 : Administration utilise 10.0.6.0/24 et non 10.0.4.0/24. Constaté
# en conditions réelles : l'instance RDS tourne dans l'AZ eu-west-3b, la même que le subnet
# technique donnees_secondary (10.0.4.0/24, cf. plus bas) — AWS refuse de détacher l'ENI de RDS
# de ce subnet tant que l'instance vit dans cette AZ (ModifyDBSubnetGroup échoue avec
# "subnets to be deleted are currently in use"), même avec create_before_destroy. Libérer
# 10.0.4.0/24 exigerait de déplacer l'instance RDS de production vers une autre AZ (Multi-AZ
# temporaire + failover, ou snapshot/restore) pour un simple réalignement de CIDR — risque jugé
# disproportionné pour une base contenant des données réelles et irremplaçables. Administration
# est donc placée sur un CIDR libre (10.0.6.0/24) plutôt que de risquer la base.

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

# --- Subnet public "DMZ" (10.0.1.0/24, cf. Bloc 1 Tableau 14) : héberge le NLB de dmz.tf ---

resource "aws_subnet" "dmz" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.project_name}-dmz-public" }
}

resource "aws_route_table_association" "dmz" {
  subnet_id      = aws_subnet.dmz.id
  route_table_id = aws_route_table.public.id
}

# --- Subnet public "Applicative" (10.0.2.0/24, cf. Bloc 1 Tableau 14) ---

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

# --- Subnet public "Administration" : héberge le bastion. CIDR 10.0.6.0/24 (écart volontaire vs
# 10.0.4.0/24 du Tableau 14, cf. commentaire en tête de fichier — contrainte AZ RDS réelle) ---

resource "aws_subnet" "administration" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.6.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.project_name}-administration-public" }
}

resource "aws_route_table_association" "administration" {
  subnet_id      = aws_subnet.administration.id
  route_table_id = aws_route_table.public.id
}

# --- Subnet privé "Données" (10.0.3.0/24, cf. Bloc 1 Tableau 14) ---

resource "aws_subnet" "donnees" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.3.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]
  tags              = { Name = "${var.project_name}-donnees-private" }
}

# Second subnet privé requis par RDS (DB subnet group exige >= 2 AZ) même en Single-AZ. Hors
# tableau 14 (pas une zone de sécurité à part entière, juste une contrainte technique RDS) :
# 10.0.4.0/24. CIDR volontairement conservé tel quel (jamais renommé) : l'instance RDS tourne
# dans l'AZ de ce subnet (eu-west-3b) et AWS refuse de détacher son ENI pour en changer — cf.
# commentaire en tête de fichier. C'est pour cette raison qu'Administration a été déplacée sur
# 10.0.6.0/24 plutôt que d'essayer de libérer 10.0.4.0/24.
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

  # Lambda de rotation Secrets Manager (secrets_rotation.tf) : a besoin d'un accès temporaire pour
  # changer le mot de passe lors de chaque rotation (tous les 30 jours).
  ingress {
    description     = "PostgreSQL depuis la Lambda de rotation Secrets Manager"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.rds_rotation_lambda.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-rds-donnees-sg" }
}
