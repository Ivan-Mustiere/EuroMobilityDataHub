# Alerte email sur échec de tâche Airflow (Bloc 1, partie 5.2/d : "Alertes Airflow : tout échec de
# tâche ETL déclenche une notification par email"). AWS SES plutôt qu'un SMTP tiers, cohérent avec
# le reste de l'infra (déjà 100% AWS). SES sandbox (compte de démo, jamais sorti du sandbox) exige
# que l'expéditeur ET le destinataire soient vérifiés : on réutilise donc la même adresse
# (var.budget_alert_email) pour les deux, une seule vérification suffit.
#
# La vérification elle-même n'est PAS automatisable : AWS envoie un email avec un lien à cliquer
# par le titulaire de l'adresse — cf. infra/README.md pour l'étape manuelle requise avant que
# l'envoi fonctionne réellement.
resource "aws_ses_email_identity" "alert" {
  email = var.budget_alert_email
}

resource "aws_iam_role_policy" "etl_service_ses_alert" {
  name = "${local.name_prefix}-etl-ses-alert"
  role = aws_iam_role.etl_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ses:SendEmail", "ses:SendRawEmail"]
        Resource = aws_ses_email_identity.alert.arn
      }
    ]
  })
}
