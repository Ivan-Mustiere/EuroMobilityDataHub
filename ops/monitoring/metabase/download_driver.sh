#!/usr/bin/env bash
# Télécharge le driver DuckDB communautaire pour Metabase.
# À exécuter UNE FOIS avant le premier `docker compose up metabase`.
#
# Source : motherduckdb/metabase_duckdb_driver (fork officiel MotherDuck)
# Versions : https://github.com/motherduckdb/metabase_duckdb_driver/releases
# Compatibilité : 1.5.4.0 → Metabase v0.62.x + DuckDB 1.5.4
#
# Usage :
#   bash monitoring/metabase/download_driver.sh           # version par défaut
#   bash monitoring/metabase/download_driver.sh 1.5.4.0   # version explicite

set -euo pipefail

VERSION="${1:-1.5.4.0}"
PLUGINS_DIR="$(cd "$(dirname "$0")/plugins" && pwd)"
JAR="${PLUGINS_DIR}/duckdb.metabase-driver.jar"
URL="https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/${VERSION}/duckdb.metabase-driver.jar"

echo "Driver DuckDB pour Metabase — version ${VERSION} (motherduckdb)"
echo "Destination : ${JAR}"
echo ""

curl -fL --progress-bar -o "${JAR}" "${URL}"

echo ""
echo "OK — driver installé. Tu peux maintenant lancer :"
echo "  docker compose up metabase"
