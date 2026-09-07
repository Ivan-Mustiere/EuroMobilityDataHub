# CloudTrail : événements de gestion uniquement (gratuit). Ne PAS activer le logging
# d'événements de données S3 (objet par objet) — facturé au volume, inutile pour une démo
# portfolio (cf. infra/README.md, section coûts).

resource "aws_s3_bucket" "cloudtrail_logs" {
  bucket = "${var.project_name}-cloudtrail-${random_id.bucket_suffix.hex}"
}

resource "aws_s3_bucket_public_access_block" "cloudtrail_logs" {
  bucket                  = aws_s3_bucket.cloudtrail_logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "cloudtrail_logs" {
  bucket = aws_s3_bucket.cloudtrail_logs.id

  rule {
    id     = "expire-30d"
    status = "Enabled"
    filter {}
    expiration {
      days = 30
    }
  }
}

data "aws_caller_identity" "current" {}

resource "aws_s3_bucket_policy" "cloudtrail_logs" {
  bucket = aws_s3_bucket.cloudtrail_logs.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AWSCloudTrailAclCheck"
        Effect    = "Allow"
        Principal = { Service = "cloudtrail.amazonaws.com" }
        Action    = "s3:GetBucketAcl"
        Resource  = aws_s3_bucket.cloudtrail_logs.arn
      },
      {
        Sid       = "AWSCloudTrailWrite"
        Effect    = "Allow"
        Principal = { Service = "cloudtrail.amazonaws.com" }
        Action    = "s3:PutObject"
        Resource  = "${aws_s3_bucket.cloudtrail_logs.arn}/AWSLogs/${data.aws_caller_identity.current.account_id}/*"
        Condition = {
          StringEquals = { "s3:x-amz-acl" = "bucket-owner-full-control" }
        }
      }
    ]
  })
}

resource "aws_cloudtrail" "main" {
  name                          = "${var.project_name}-trail"
  s3_bucket_name                = aws_s3_bucket.cloudtrail_logs.id
  include_global_service_events = true
  is_multi_region_trail         = false
  enable_log_file_validation    = true

  depends_on = [aws_s3_bucket_policy.cloudtrail_logs]
}

# GuardDuty : essai gratuit 30 jours, couvre toute la durée du build (cf. infra/README.md).
# Optionnel (var.enable_guardduty) : certains comptes AWS "Free Plan" renvoient
# SubscriptionRequiredException tant que le compte n'est pas passé en plan payant complet.
resource "aws_guardduty_detector" "main" {
  count  = var.enable_guardduty ? 1 : 0
  enable = true
}
