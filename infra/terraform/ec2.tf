# Instance "Applicative" (Bloc 1, Tableau 13) : héberge Kafka + le producer/consumer GTFS-RT +
# Airflow (DAG batch ingest/transform/load_cloud), via Docker. Zéro accès SSH : administration
# exclusivement par SSM (cf. iam.tf, vpc.tf), le rôle etl_service porte déjà les permissions S3 +
# Secrets Manager nécessaires au bootstrap (cf. user_data).

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Empaqueté à chaque `terraform plan/apply` (pas d'étape manuelle) : le DAG Airflow exécute
# pipeline/run.py + pipeline/load_cloud.py + dbt directement (cf. ../cloud/docker-compose.yml,
# volumes ../pipeline, ../config et ../dbt relatifs à cloud/docker-compose.yml), donc le bundle
# doit contenir cloud/ ET pipeline/ ET config/ ET dbt/ avec la même disposition relative que dans
# le dépôt — d'où cette étape de mise en scène (staging) avant l'archivage.
resource "null_resource" "stage_cloud_bundle" {
  triggers = { always_run = timestamp() }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      rm -rf "${path.module}/.staging"
      mkdir -p "${path.module}/.staging"
      cp -r "${path.module}/../../cloud" "${path.module}/.staging/cloud"
      rm -rf "${path.module}/.staging/cloud/local-test"
      cp -r "${path.module}/../../pipeline" "${path.module}/.staging/pipeline"
      cp -r "${path.module}/../../config" "${path.module}/.staging/config"
      cp -r "${path.module}/../../dbt" "${path.module}/.staging/dbt"
      rm -rf "${path.module}/.staging/dbt/target" "${path.module}/.staging/dbt/dbt_packages" "${path.module}/.staging/dbt/logs"
      find "${path.module}/.staging" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    EOT
  }
}

data "archive_file" "cloud_bundle" {
  type        = "zip"
  source_dir  = "${path.module}/.staging"
  output_path = "${path.module}/.terraform-cloud-bundle.zip"
  depends_on  = [null_resource.stage_cloud_bundle]
}

resource "aws_s3_object" "cloud_bundle" {
  bucket = aws_s3_bucket.bronze.bucket
  key    = "_deploy/cloud.zip"
  source = data.archive_file.cloud_bundle.output_path
  etag   = data.archive_file.cloud_bundle.output_md5
}

resource "aws_instance" "applicative" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.ec2_instance_type
  subnet_id              = aws_subnet.applicative.id
  vpc_security_group_ids = [aws_security_group.ec2_applicative.id]
  iam_instance_profile   = aws_iam_instance_profile.etl_service.name

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = templatefile("${path.module}/templates/ec2_user_data.sh.tftpl", {
    bucket       = aws_s3_bucket.bronze.bucket
    region       = var.aws_region
    project_name = var.project_name
  })
  # Un changement de user_data ne recrée pas l'instance par défaut (Terraform ne le réexécute
  # qu'au prochain boot) : accepté ici, le bootstrap est idempotent et cette instance est jetable
  # (détruite en fin de build, cf. infra/README.md).

  depends_on = [
    aws_secretsmanager_secret_version.etl_cloud_credentials,
    aws_secretsmanager_secret_version.rds_credentials,
    aws_s3_object.cloud_bundle,
    aws_db_instance.donnees,
  ]

  tags = { Name = "${var.project_name}-applicative" }
}
