#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1
build/builder.sh make .bootstrap # explicitly recompile teamcity-trigger
build/builder.sh env TC_API_USER="$TC_API_USER" TC_API_PASSWORD="$TC_API_PASSWORD" TC_SERVER_URL="$TC_SERVER_URL" teamcity-trigger
