# RDS PostgreSQL — couche Silver temps réel (Bloc 1, Tableau 5) : reçoit en continu les mises à
# jour écrites par les consommateurs Kafka (cf. cloud/consumer.py, ec2.tf). Mot de passe généré
# et stocké dans un secret dédié (pas le secret Snowflake de secrets.tf) pour permettre une
# rotation indépendante.

resource "aws_db_subnet_group" "donnees" {
  name       = "${local.name_prefix}-donnees"
  subnet_ids = [aws_subnet.donnees.id, aws_subnet.donnees_secondary.id]
}

# SSL obligatoire Applicative -> Données (Bloc 1, partie 5.2/b : "PostgreSQL via SSL obligatoire,
# sslmode=require"). rds.force_ssl est un paramètre dynamique (pas de redémarrage requis) : le
# serveur rejette toute connexion en clair dès l'application du paramètre.
resource "aws_db_parameter_group" "donnees" {
  name        = "${local.name_prefix}-donnees-pg16"
  family      = "postgres16"
  description = "Force SSL sur les connexions PostgreSQL (Bloc 1, partie 5.2/b)"

  parameter {
    name  = "rds.force_ssl"
    value = "1"
  }
}

resource "random_password" "rds_master" {
  length  = 24
  special = false # évite les caractères qui nécessitent un échappement dans une URL de connexion
}

resource "aws_secretsmanager_secret" "rds_credentials" {
  name        = "${local.name_prefix}/rds-postgres"
  description = "Identifiants de connexion RDS PostgreSQL (couche Silver temps réel)"
}

# Format JSON standard attendu par la Lambda de rotation AWS (voir secrets_rotation.tf) : la
# rotation "single user" gère elle-même le cycle password ancien/nouveau et écrase la valeur
# "password" ci-dessous à chaque rotation (dernier bloc "AWSCURRENT" du secret). Les clés
# PG*/majuscules attendues par consumer.py et le bootstrap EC2 sont reconstruites à partir de
# celles-ci (cf. ec2_user_data.sh.tftpl), jamais l'inverse.
resource "aws_secretsmanager_secret_version" "rds_credentials" {
  secret_id = aws_secretsmanager_secret.rds_credentials.id
  secret_string = jsonencode({
    engine   = "postgres"
    host     = aws_db_instance.donnees.address
    port     = aws_db_instance.donnees.port
    dbname   = aws_db_instance.donnees.db_name
    username = aws_db_instance.donnees.username
    password = random_password.rds_master.result
  })

  # La rotation (secrets_rotation.tf) modifie ce secret en dehors de Terraform : ignorer les
  # dérives sur secret_string évite qu'un apply ultérieur n'écrase le mot de passe tourné par
  # Lambda avec l'ancien random_password.rds_master figé dans le state.
  lifecycle {
    ignore_changes = [secret_string]
  }
}

resource "aws_db_instance" "donnees" {
  identifier     = "${local.name_prefix}-donnees"
  engine         = "postgres"
  engine_version = "16.15"
  instance_class = var.rds_instance_class

  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  db_name  = "euromobilitydatahub"
  username = "etl_admin"
  password = random_password.rds_master.result

  db_subnet_group_name   = aws_db_subnet_group.donnees.name
  parameter_group_name   = aws_db_parameter_group.donnees.name
  vpc_security_group_ids = [aws_security_group.rds_donnees.id]
  publicly_accessible    = false
  multi_az               = false # Single-AZ assumé (cf. vpc.tf) — build démo, pas un SLA de prod

  # Compromis démo assumés (voir infra/README.md, section coûts/destroy) : pas de sauvegarde
  # automatique et pas de snapshot final — les données sont régénérables depuis S3/Kafka, cf.
  # justification de la traçabilité Bronze->Silver->Gold (Bloc 1, partie 3.1).
  backup_retention_period = 0
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true

  # La rotation Secrets Manager (secrets_rotation.tf) change le mot de passe réel sur RDS
  # directement, hors Terraform : sans ceci, le prochain apply réécraserait ce mot de passe tourné
  # avec l'ancien random_password.rds_master figé dans le state, cassant l'accès pour de vrai.
  lifecycle {
    ignore_changes = [password]
  }
}
