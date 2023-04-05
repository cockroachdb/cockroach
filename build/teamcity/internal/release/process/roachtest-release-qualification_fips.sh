#!/usr/bin/env bash

set -euo pipefail

FIPS_ENABLED=1 ./build/teamcity/internal/release/process/roachtest-release-qualification.sh
