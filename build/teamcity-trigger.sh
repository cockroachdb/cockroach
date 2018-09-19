#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

run build/builder.sh go install ./pkg/cmd/teamcity-trigger
run build/builder.sh env \
  TC_API_USER="$TC_API_USER" \
  TC_API_PASSWORD="$TC_API_PASSWORD" \
  TC_SERVER_URL="$TC_SERVER_URL" \
  TC_BUILD_BRANCH="$TC_BUILD_BRANCH" \
  teamcity-trigger "$@"
