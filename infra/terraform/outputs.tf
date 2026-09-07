output "vpc_id" {
  value = aws_vpc.main.id
}

output "applicative_subnet_id" {
  value = aws_subnet.applicative.id
}

output "donnees_subnet_ids" {
  value = [aws_subnet.donnees.id, aws_subnet.donnees_secondary.id]
}

output "ec2_applicative_sg_id" {
  value = aws_security_group.ec2_applicative.id
}

output "rds_donnees_sg_id" {
  value = aws_security_group.rds_donnees.id
}

output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze.bucket
}

output "etl_service_instance_profile" {
  value = aws_iam_instance_profile.etl_service.name
}

output "guardduty_detector_id" {
  value = var.enable_guardduty ? aws_guardduty_detector.main[0].id : null
}

output "snowflake_database_name" {
  value = snowflake_database.main.name
}

output "snowflake_warehouse_name" {
  value = snowflake_warehouse.main.name
}

output "snowflake_bronze_stage" {
  value = snowflake_stage_external_s3.bronze.fully_qualified_name
}

output "snowflake_storage_integration_iam_user_arn" {
  description = "A comparer avec le Principal de la trust policy de aws_iam_role.snowflake_s3_access — permet de vérifier l'intégration sans DESC STORAGE INTEGRATION manuel"
  value       = snowflake_storage_integration_aws.bronze.describe_output[0].iam_user_arn
}
