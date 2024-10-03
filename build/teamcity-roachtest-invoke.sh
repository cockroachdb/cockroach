#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

set +e
# Append any given command-line parameters. If a switch listed below is also
# passed by the caller, the passed one takes precedence.
bin/roachtest run \
  --teamcity \
  --os-volume-size=32 \
  "$@"
code=$?
set -e

if [[ ${code} == 10 || ${code} == 11 ]]; then
  # Exit code 10 indicates that some tests failed; exit code 11
  # indicates that cluster creation failed for some test in the
  # run. In both cases, roachtest as a whole passed. We want to exit
  # zero in this case so that we can let TeamCity report failing tests
  # without also failing the build. That way, build failures can be
  # used to notify about serious problems that prevent tests from
  # being invoked in the first place (typically exit code 1).
  code=0
fi

exit ${code}
