#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Lint"
# Disable ccache so that Go doesn't try to install dependencies into GOROOT,
# where it doesn't have write permissions. (Using ccache busts the Go package
# cache because it appears to the Go toolchain as a different C compiler than
# the toolchain was compiled with.) We've already built the C dependencies
# above, so we're not losing anything by turning it off.
#
# TODO(benesch): once GOPATH/pkg goes away because Go static analysis tools can
# rebuild on demand, remove this. Upstream issue: golang/go#25650.
run_json_test env COCKROACH_BUILDER_CCACHE= \
  build/builder.sh stdbuf -eL -oL make GOTESTFLAGS=-json lint
tc_end_block "Lint"
