#!/usr/bin/env bash
set -euxo pipefail

build_dir="$(dirname $0)"

"${build_dir}"/builder.sh make .bootstrap # explicitly recompile teamcity-trigger
"${build_dir}"/builder.sh env TC_API_USER="$TC_API_USER" TC_API_PASSWORD="$TC_API_PASSWORD" TC_SERVER_URL="$TC_SERVER_URL" teamcity-trigger
