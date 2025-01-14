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

while [ -z $pr ] || ! [[ $pr =~ ^[0-9]+$ ]]; do
	read -p 'PR number (defaults to first arg): ' pr
done

tests=${2-}

while [ -z $tests ]; do
	read -p 'Regexp (defaults to second arg): ' tests
done

json_payload=$(jq -n \
  --arg branch_name "$pr" \
  --arg tests "$tests" \
  --arg branch_name "$pr" \
  '
{
  buildType: {id: "Cockroach_Nightlies_RoachtestNightlyGceBazel"},
  branchName: $branch_name,
  properties: {
    property: [
      {name: "env.ARM_PROBABILITY", value: "0"},
      {name: "env.COCKROACH_EA_PROBABILITY", value: "0"},
      {name: "env.DEBUG", value: "true"},
      {name: "env.TESTS", value: $tests}
    ]
  }
}')

curl -ss -X POST \
  https://teamcity.cockroachdb.com/app/rest/buildQueue \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H "Authorization: Bearer $token" \
  -d "$json_payload" | jq -r '.webUrl'


