#!/usr/bin/env bash
set -euxo pipefail

build/builder.sh env TC_API_USER="$TC_API_USER" TC_API_PASSWORD="$TC_API_PASSWORD" teamcity-trigger
