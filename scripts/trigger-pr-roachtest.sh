#!/usr/bin/env bash

# Copyright 2016 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

token=${TEAMCITY_TOKEN-}
if [ -z $token ]; then
  cat <<EOF
TEAMCITY_TOKEN not set. Get one here:

https://teamcity.cockroachdb.com/profile.html?item=accessTokens
EOF
  exit 1
fi

pr=${1-}

while [ -z $pr ]; do
  read -p 'PR number (defaults to first arg, can also use branch name): ' pr
done

tests=${2-}

while [ -z "$tests" ]; do
  read -p 'Test Regexp (defaults to second arg): ' tests
done

sha=${3-}
if [ -z "${sha}" ]; then
  read -p 'Long Commit SHA (defaults to third arg, empty for latest changes): ' sha
fi

dump_json() {
  if echo "$1" | jq '.' >/dev/null 2>&1; then
    echo "$1" | jq '.'
  else
    echo "$1"
  fi
}

json_payload=$(jq -n \
  --arg branch_name "$pr" \
  --arg tests "$tests" \
  --arg envDebug "${DEBUG-false}" \
  --arg envCount "${COUNT-1}" \
  '{
  buildType: {id: "Cockroach_Nightlies_RoachtestNightlyGceBazel"},
  branchName: $branch_name,
  properties: {
    property: [
      {name: "env.ARM_PROBABILITY", value: "0"},
      {name: "env.COCKROACH_EA_PROBABILITY", value: "0"},
      {name: "env.DEBUG", value: $envDebug},
      {name: "env.COUNT", value: $envCount},
      {name: "env.TESTS", value: $tests}
    ]
  }
}')

if [ -n "${sha}" ]; then
  json_payload=$(echo "${json_payload}" | jq \
    --arg branch_name "$pr" \
    --arg sha "$sha" \
    '. + {
  revisions: {
    revision: [
      {
        version: $sha,
        vcsBranchName: ("refs/heads/" + $branch_name),
        "vcs-root-instance": {
          "vcs-root-id": "Cockroach_Cockroach"
        }
      }
    ]
  }
}')
fi

echo "Input:"
dump_json "$json_payload"
echo "======================================="
echo "Output:"
result=$(curl -ss -X POST \
  https://teamcity.cockroachdb.com/app/rest/buildQueue \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H "Authorization: Bearer $token" \
  -d "$json_payload")
dump_json "$result"
echo "======================================="
echo "${result}" | jq -r '.webUrl' 2>/dev/null
