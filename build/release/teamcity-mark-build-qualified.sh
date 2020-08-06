#!/usr/bin/env bash

set -euxo pipefail

source "$(dirname "${0}")/teamcity-mark-build.sh"

mark_build "qualified"
