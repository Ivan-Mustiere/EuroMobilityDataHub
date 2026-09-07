# Zone "Administration" (Bloc 1, Tableau 14) : VPN WireGuard puis bastion SSH — coexiste avec
# SSM (cf. vpc.tf) plutôt que de le remplacer. Clés SSH et WireGuard générées localement hors
# Terraform (jamais les clés privées dans le state) : seules les clés PUBLIQUES/celle du pair
# distant sont lues ici. La clé privée serveur WireGuard reste nécessaire dans le user_data
# (chiffré en transit vers l'instance, comme tout user_data EC2) pour configurer wg0.

resource "aws_key_pair" "bastion" {
  key_name   = "${local.name_prefix}-bastion"
  public_key = trimspace(file(pathexpand("~/.ssh/euromobilitydatahub_bastion_key${local.env_suffix}.pub")))
}

# Seul le port VPN est ouvert au public : SSH (22) n'écoute plus que sur l'interface WireGuard
# (10.99.0.1, cf. bastion_user_data.sh.tftpl) — d'où l'absence de règle ingress sur le port 22.
resource "aws_security_group" "bastion" {
  name        = "${local.name_prefix}-bastion"
  description = "Bastion SSH (zone Administration) - acces restreint a my_ip_cidr"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "VPN WireGuard depuis my_ip_cidr uniquement, seul chemin vers SSH (tunnel 10.99.0.0/24)"
    from_port   = 51820
    to_port     = 51820
    protocol    = "udp"
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

  user_data = templatefile("${path.module}/templates/bastion_user_data.sh.tftpl", {
    wg_server_private_key = trimspace(file(pathexpand("~/.wireguard/server_private${local.env_suffix}.key")))
    wg_client_public_key  = trimspace(file(pathexpand("~/.wireguard/client_public${local.env_suffix}.key")))
  })

  tags = { Name = "${local.name_prefix}-bastion" }
}
