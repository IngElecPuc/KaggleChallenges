#!/usr/bin/env bash
set -euo pipefail
DB="${1:-saml-d}"
/usr/bin/cypher-shell -u neo4j -p 'Banco.69' -d system \
"STOP DATABASE \`$DB\` YIELD name; DROP DATABASE \`$DB\` IF EXISTS;"

