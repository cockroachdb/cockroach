#!/bin/bash
# Build cockroach binary using jemalloc as the allocator.

set -euo pipefail

source $(dirname $0)/build-common.sh

time make STATIC=1 build TAGS="jemalloc"
check_static cockroach
strip -S cockroach
