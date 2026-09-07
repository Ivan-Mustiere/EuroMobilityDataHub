# Garde-fou coût — à appliquer en tout premier (avant tout autre `terraform apply`), voir
# infra/README.md. Alerte email simple (pas de SNS) dès que le coût prévisionnel dépasse 80 %
# du seuil, et à 100 %.

resource "aws_budgets_budget" "monthly_guardrail" {
  name         = "${var.project_name}-guardrail"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }
}
