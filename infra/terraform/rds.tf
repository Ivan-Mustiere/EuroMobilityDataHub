# RDS PostgreSQL — couche Silver temps réel (Bloc 1, Tableau 5) : reçoit en continu les mises à
# jour écrites par les consommateurs Kafka (cf. cloud/consumer.py, ec2.tf). Mot de passe généré
# et stocké dans un secret dédié (pas le secret Snowflake de secrets.tf) pour permettre une
# rotation indépendante.

resource "aws_db_subnet_group" "donnees" {
  name       = "${var.project_name}-donnees"
  subnet_ids = [aws_subnet.donnees.id, aws_subnet.donnees_secondary.id]
}

resource "random_password" "rds_master" {
  length  = 24
  special = false # évite les caractères qui nécessitent un échappement dans une URL de connexion
}

resource "aws_secretsmanager_secret" "rds_credentials" {
  name        = "${var.project_name}/rds-postgres"
  description = "Identifiants de connexion RDS PostgreSQL (couche Silver temps réel)"
}

resource "aws_secretsmanager_secret_version" "rds_credentials" {
  secret_id = aws_secretsmanager_secret.rds_credentials.id
  secret_string = jsonencode({
    PGHOST     = aws_db_instance.donnees.address
    PGPORT     = aws_db_instance.donnees.port
    PGDATABASE = aws_db_instance.donnees.db_name
    PGUSER     = aws_db_instance.donnees.username
    PGPASSWORD = random_password.rds_master.result
  })
}

resource "aws_db_instance" "donnees" {
  identifier     = "${var.project_name}-donnees"
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
}
