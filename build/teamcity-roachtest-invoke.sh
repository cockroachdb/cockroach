#!/usr/bin/env bash
set -euo pipefail

set +e
# Append any given command-line parameters. If a switch listed below is also
# passed by the caller, the passed one takes precedence.
bin/roachtest run \
  --teamcity \
  --roachprod="${PWD}/bin/roachprod" \
  --workload="${PWD}/bin/workload" \
  --create-args=--os-volume-size=32 \
  "$@"
code=$?
set -e

if [[ ${code} -eq 10 ]]; then
  # Exit code 10 indicates that some tests failed, but that roachtest
  # as a whole passed. We want to exit zero in this case so that we
  # can let TeamCity report failing tests without also failing the
  # build. That way, build failures can be used to notify about serious
  # problems that prevent tests from being invoked in the first place.
  code=0
fi

exit ${code}
