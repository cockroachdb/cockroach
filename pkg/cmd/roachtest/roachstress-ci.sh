#!/usr/bin/env bash
set -euo pipefail

# This script spawns a Roachtest-stress CI job. You need a TeamCity token:
#
# https://www.jetbrains.com/help/teamcity/managing-your-user-account.html#Managing+Access+Tokens
#
# which the script assumes is available from the TEAMCITY_TOKEN environment variable.

if [ -z "${BASH_VERSINFO}" ] || [ -z "${BASH_VERSINFO[0]}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  echo "This script requires Bash version >= 4"
  echo "On OSX, 'brew install bash' should do the trick."
  exit 1
fi

if [ ! -v TEAMCITY_TOKEN ]; then
  echo "env var TEAMCITY_TOKEN must be set"
  exit 1
fi

# Read user input.
if [ ! -v BRANCH ]; then read -r -e -i "master" -p "Branch: " BRANCH; fi
if [ ! -v SHA ]; then read -r -e -p "SHA (empty for latest): " SHA; fi
if [ ! -v TEST ]; then read -r -e -p "Test regexp: " TEST; fi
if [ ! -v COUNT ]; then read -r -e -i "10" -p "Count: " COUNT; fi

curl -X POST \
  -H "Authorization: Bearer ${TEAMCITY_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  --data-binary @- \
  https://teamcity.cockroachdb.com/app/rest/buildQueue <<EOF
{
  "branchName": "${BRANCH}",
  "buildType": {
    "id": "Cockroach_Nightlies_RoachtestStress"
  },
  "comment": {
    "text": "roachstress-ci.sh ${BRANCH} ${SHA} ${TEST} ${COUNT}"
  },
  "lastChanges": {
    "change": [
      {
        "locator": "buildType:(id:(Cockroach_Nightlies_RoachtestStress)),version:${SHA}"
      }
    ]
  },
  "properties": {
    "property": [
      {
        "name": "env.COUNT",
        "value": "${COUNT}"
      },
      {
        "name": "env.TESTS",
        "value": "${TEST}"
      }
    ]
  }
}
EOF

echo "

Build should be listed under https://teamcity.cockroachdb.com/viewType.html?buildTypeId=Cockroach_Nightlies_RoachtestStress&tab=buildTypeStatusDiv"
