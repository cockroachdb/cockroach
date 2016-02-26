#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 2 ]; then
  cat <<EOF
Trigger a CircleCI nightly build for the given branch.

  Usage: $0 <circle_api_token> <ref>
EOF
  exit 2
fi

REV="${2}"
echo "Triggering ${REV}"
curl -X POST --header 'Content-Type: application/json' \
  -d ' { "build_parameters": { "BUILD_OSX": "true" } }' \
  "https://circleci.com/api/v1/project/cockroachdb/cockroach/tree/${REV}?circle-token=${1}"
