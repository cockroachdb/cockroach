#!/bin/bash

set -euo pipefail

apt-get update
apt-get install --yes jq
if pro status --format json | jq -e '.services[] | select(.name == "fips") | select(.status == "enabled")'; then
  echo "FIPS is already enabled"
else
  ua enable fips --assume-yes
fi
apt-get install -y dpkg-repack
