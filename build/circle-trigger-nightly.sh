#!/bin/bash
set -euo pipefail

if [ $# -ne 2 ]; then
  cat <<EOF
Trigger a CircleCI nightly build for the given ref (for example "master").

  Usage: $0 <circle_api_token> <ref>
EOF
  exit 2
fi

REV=$(git ls-remote --exit-code git@github.com:cockroachdb/cockroach.git "${2}" | awk '{print $1}')
echo "Triggering ${2} (${REV})"
curl -X POST --header 'Content-Type: application/json' \
  -d ' { "build_parameters": { "NIGHTLY": "true" } }' \
  "https://circleci.com/api/v1/project/cockroachdb/cockroach/tree/${REV}?circle-token=${1}"
