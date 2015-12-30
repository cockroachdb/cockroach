#!/bin/bash
set -eu

REV=$(git describe --tags)

if [ -z "${1-}" ]; then
  cat <<EOF
Trigger a CircleCI nightly build for the revision currently checked out (${REV}).

  Usage: $0 <circle_api_token>
EOF
  exit 2
fi

curl -X POST --header 'Content-Type: application/json' \
  -d ' { "build_parameters": { "NIGHTLY": "true" } }' \
  "https://circleci.com/api/v1/project/cockroachdb/cockroach/tree/${REV}?circle-token=${1}"
