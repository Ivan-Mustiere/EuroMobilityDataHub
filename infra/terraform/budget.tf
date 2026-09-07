# Garde-fou coût — à appliquer en tout premier (avant tout autre `terraform apply`), voir
# infra/README.md. Alerte email simple (pas de SNS) dès que le coût prévisionnel dépasse 80 %
# du seuil, et à 100 %.
#
# Un seul budget pour tout le compte (pas de filtre par tag/service) : le créer une seconde fois
# depuis le workspace "prod" surveillerait exactement la même dépense totale du compte, en double
# — count le limite au workspace preprod.
resource "aws_budgets_budget" "monthly_guardrail" {
  count        = local.is_preprod ? 1 : 0
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
