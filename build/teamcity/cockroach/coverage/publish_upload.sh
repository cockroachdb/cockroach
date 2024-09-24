#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# This script is the third step of the "Publish Coverage" build.
#
# It takes the HTML mini-websites produced by the previous step and uploads them
# to GCE. It also updates the index.html file listing all uploaded profiles.
# The index can be accessed at:
#   https://storage.googleapis.com/crl-codecover-public/cockroach/index.html

BUCKET=crl-codecover-public

set -euo pipefail

source "build/teamcity-support.sh" # for log_into_gcloud

google_credentials="$GOOGLE_CREDENTIALS"
log_into_gcloud

gsutil ls gs://$BUCKET/

TIMESTAMP=$(date -u '+%Y-%m-%d %H:%MZ')

publish() {
  PROFILE="$1"

  DIR="$TIMESTAMP $(git rev-parse --short=8 HEAD) - $PROFILE"
  echo "Uploading to $DIR.."
  gsutil -m cp -Z -r "output/html/$PROFILE" "gs://$BUCKET/cockroach/$DIR" > "output/logs/upload-$PROFILE.log" 2>&1
}

for dir in $(find output/html -mindepth 1 -maxdepth 1 -type d); do
  publish $(basename "$dir")
done

# Regenerate index.html.
INDEX=$(mktemp)
trap "rm -f $INDEX" EXIT

echo '<title>Cockroach coverage</title><body><h2>Cockroach coverage runs:</h2><ul>' > "$INDEX"
gsutil ls "gs://$BUCKET/cockroach" |
  sed "s#gs://$BUCKET/cockroach/##" |
  sed 's#/$##' |
  grep -v index.html |
  sort -r |
  while read -r d; do
    echo "<li><a href=\"$d/index.html\">$d</a>" >> "$INDEX"
  done

echo '</ul></body>' >> "$INDEX"

gsutil \
  -h "Cache-Control:public, max-age=300, no-transform" \
  -h "Content-Type:text/html" \
  cp "$INDEX" "gs://$BUCKET/cockroach/index.html"
