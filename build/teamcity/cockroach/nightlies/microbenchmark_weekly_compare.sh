#!/usr/bin/env bash
#
# This script compares output from roachprod-microbench runs and outputs the
# results to a Google Sheet.
# Parameters:
#   COMPARE_NEW_DIR: local path to the new roachprod-microbench output directory (default: ./artifacts/microbench/0)
#   COMPARE_OLD_DIR: local path to the old roachprod-microbench output directory (default: ./artifacts/microbench/1)
#   SHEET_DESCRIPTION: Adds a description to the name of the published spreadsheets (e.g., "22.2 -> 22.1")

set -exuo pipefail

# Set up credentials
dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"

# Generate sheets
./bin/roachprod-microbench compare "$COMPARE_NEW_DIR" "$COMPARE_OLD_DIR" --sheet-desc="$SHEET_DESCRIPTION"
