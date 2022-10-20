#!/usr/bin/env bash

set -euo pipefail

build/teamcity/cockroach/ci/tests/local_roachtest.sh
