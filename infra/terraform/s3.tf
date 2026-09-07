# Bucket Bronze (Bloc 1, partie 3.1/a) : stockage brut des CSV SNCF avant COPY INTO Snowflake.

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-bronze-${random_id.bucket_suffix.hex}"
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256" # SSE-S3, cf. Bloc 1 Table 15
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Rétention 30 jours sur le préfixe logs/ (RGPD, cf. Bloc 1 partie 5.1/d — cohérence narrative
# avec la politique de rétention des logs d'accès API, même si l'infra entière est détruite
# après une semaine).
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "expire-logs-30d"
    status = "Enabled"
    filter {
      prefix = "logs/"
    }
    expiration {
      days = 30
    }
  }
}
