#!/usr/bin/env bash
set -euo pipefail
DB="${1:-saml-d}"
/usr/bin/cypher-shell -u neo4j -p 'Banco.69' -d system \
"CREATE DATABASE \`$DB\` IF NOT EXISTS; SHOW DATABASES WHERE name = '$DB';"

