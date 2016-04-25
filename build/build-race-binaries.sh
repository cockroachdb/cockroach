#!/bin/bash
# Build cockroach binary with race detection enabled.

set -euo pipefail

source $(dirname $0)/build-common.sh

time make STATIC=1 build GOFLAGS="-race"
check_static cockroach
strip -S cockroach
