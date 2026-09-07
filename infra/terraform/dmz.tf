# Zone "DMZ" (Bloc 1, Tableau 13) : API Gateway HTTP API, seul point d'entrée public vers l'API
# FastAPI. Intégration privée via VPC Link + Network Load Balancer interne (pas de nom DNS/IP
# publique) : l'instance Applicative devient injoignable directement depuis internet — voir la
# règle retirée dans vpc.tf (aws_security_group.ec2_applicative) et la nouvelle limitée au VPC.

resource "aws_lb" "api_internal" {
  name               = "${local.name_prefix}-api-nlb"
  internal           = true # pas d'IP publique : joignable uniquement via le VPC Link
  load_balancer_type = "network"
  subnets            = [aws_subnet.applicative.id]

  tags = { Name = "${local.name_prefix}-api-nlb" }
}

resource "aws_lb_target_group" "api" {
  name        = "${local.name_prefix}-api-tg"
  port        = 8000
  protocol    = "TCP"
  vpc_id      = aws_vpc.main.id
  target_type = "instance"

  health_check {
    protocol = "TCP"
    port     = "8000"
  }

  tags = { Name = "${local.name_prefix}-api-tg" }
}

resource "aws_lb_target_group_attachment" "api" {
  target_group_arn = aws_lb_target_group.api.arn
  target_id        = aws_instance.applicative.id
  port             = 8000
}

resource "aws_lb_listener" "api" {
  load_balancer_arn = aws_lb.api_internal.arn
  port              = 8000
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

# ENIs du VPC Link : pas d'ingress nécessaire (API Gateway les utilise en sortant vers le NLB),
# egress libre pour atteindre le NLB dans le VPC.
resource "aws_security_group" "vpc_link" {
  name        = "${local.name_prefix}-vpc-link"
  description = "ENIs du VPC Link API Gateway vers NLB interne (zone DMZ)"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name_prefix}-vpc-link-sg" }
}

resource "aws_apigatewayv2_vpc_link" "main" {
  name               = "${local.name_prefix}-vpc-link"
  security_group_ids = [aws_security_group.vpc_link.id]
  subnet_ids         = [aws_subnet.applicative.id]

  tags = { Name = "${local.name_prefix}-vpc-link" }
}

resource "aws_apigatewayv2_api" "main" {
  name          = "${local.name_prefix}-api"
  protocol_type = "HTTP"
  description   = "DMZ - point d'entree public de l'API cloud (Bloc 1, Tableau 13)"
}

resource "aws_apigatewayv2_integration" "api" {
  api_id           = aws_apigatewayv2_api.main.id
  description      = "Integration privee vers le NLB interne (zone Applicative), via VPC Link"
  integration_type = "HTTP_PROXY"
  integration_uri  = aws_lb_listener.api.arn

  integration_method = "ANY"
  connection_type    = "VPC_LINK"
  connection_id      = aws_apigatewayv2_vpc_link.main.id
}

resource "aws_apigatewayv2_route" "proxy" {
  api_id    = aws_apigatewayv2_api.main.id
  route_key = "ANY /{proxy+}"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_apigatewayv2_route" "root" {
  api_id    = aws_apigatewayv2_api.main.id
  route_key = "ANY /"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.main.id
  name        = "$default"
  auto_deploy = true
}
