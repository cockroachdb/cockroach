#!/usr/bin/env bash
set -euxo pipefail

build/builder.sh env TC_API_USER="$TC_API_USER" TC_API_PASSWORD="$TC_API_PASSWORD" teamcity-trigger -build Cockroach_UnitTests -add-params env.COCKROACH_PROPOSER_EVALUATED_KV=true -pkgs ""
