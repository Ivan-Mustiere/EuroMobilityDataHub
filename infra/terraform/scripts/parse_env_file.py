"""Data source externe Terraform (secrets.tf) : lit un fichier KEY=VALUE (infra/cloud/local-test/
*.env) et le renvoie en JSON, pour que les vrais tokens GTFS-RT committés dans ces fichiers restent
la SEULE source de vérité (pas de duplication dans le HCL) — réutilisés ici pour peupler les
secrets Secrets Manager que l'EC2 réel va effectivement consommer (cf. ec2_user_data.sh.tftpl).

Protocole "external" data source : lit un objet JSON sur stdin (ignoré ici, cf. query.path),
renvoie un objet plat {clé: valeur} en JSON sur stdout.
"""

import json
import sys

query = json.load(sys.stdin)
path = query["path"]

result = {}
with open(path, encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        result[key.strip()] = value.strip()

json.dump(result, sys.stdout)
