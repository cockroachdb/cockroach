#!/usr/bin/env bash

set -euo pipefail

build/teamcity/cockroach/ci/tests/maybe_stress.sh
