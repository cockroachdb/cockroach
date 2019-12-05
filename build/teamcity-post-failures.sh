#!/usr/bin/env bash
# This script is invoked from bleeding edge builds. It invokes a helper (tc2gh)
# that uses the TC API to transitively pipe all of the logs for failing
# dependencies of the current build.
#
# Various env vars must be set for all of this to work. They are generally
# available in the bleeding edge build (or the error will print which one is
# missing).
#
# TODO(tbg): clean that up.

set -euo pipefail
# TODO(tbg): stop swallowing exit status once the kinks have been worked out.
if ! go run ./pkg/cmd/tc2gh "${TC_BUILD_ID}"; then
  echo "tc2gh failed, continuing anyway"
fi

