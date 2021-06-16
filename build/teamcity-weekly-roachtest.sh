#!/usr/bin/env bash

# Note that when this script is called, the cockroach binary to be tested
# already exists in the current directory.

set -euo pipefail

google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
source "$(dirname "${0}")/teamcity-support.sh"
log_into_gcloud
gcloud auth list --format=json --filter 'status~ACTIVE'
