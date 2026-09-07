# Zone "Administration" (Bloc 1, Tableau 13) : bastion SSH, coexiste avec SSM plutôt que de le
# remplacer (cf. vpc.tf). Clé SSH générée localement hors Terraform (jamais dans le state, à la
# différence d'un tls_private_key qui écrirait la clé privée en clair dans le state) — seule la
# clé PUBLIQUE est lue ici.

resource "aws_key_pair" "bastion" {
  key_name   = "${local.name_prefix}-bastion"
  public_key = trimspace(file(pathexpand("~/.ssh/euromobilitydatahub_bastion_key${local.env_suffix}.pub")))
}

# Zéro accès entrant sauf SSH depuis my_ip_cidr : c'est tout le rôle du bastion, pas de règle
# supplémentaire à ouvrir.
resource "aws_security_group" "bastion" {
  name        = "${local.name_prefix}-bastion"
  description = "Bastion SSH (zone Administration) - acces restreint a my_ip_cidr"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH depuis my_ip_cidr uniquement"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
  }

  egress {
    description = "Sortant libre (mises a jour OS, psql vers RDS)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name_prefix}-bastion-sg" }
}

resource "aws_instance" "bastion" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.bastion_instance_type
  subnet_id              = aws_subnet.administration.id
  vpc_security_group_ids = [aws_security_group.bastion.id]
  key_name               = aws_key_pair.bastion.key_name

  root_block_device {
    volume_size = 8
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = file("${path.module}/templates/bastion_user_data.sh.tftpl")

  tags = { Name = "${local.name_prefix}-bastion" }
}
