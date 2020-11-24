#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$PWD/artifacts/testrace
mkdir -p "$TMPDIR"

tc_start_block "Determine changed packages"
if tc_release_branch; then
  pkgspec=./pkg/...
  echo "On release branch ($TC_BUILD_BRANCH), so running testrace on all packages ($pkgspec)"
else
  pkgspec=$(changed_go_pkgs)
  if [[ $(echo "$pkgspec" | wc -w) -gt 10 ]]; then
    echo "PR #$TC_BUILD_BRANCH changed many packages; skipping race detector tests"
    exit 0
  elif [[ -z "$pkgspec" ]]; then
    echo "PR #$TC_BUILD_BRANCH has no changed packages; skipping race detector tests"
    exit 0
  fi
  if [[ $pkgspec == *"./pkg/sql/opt"* ]]; then
    # If one opt package was changed, run all opt packages (the optimizer puts
    # various checks behind the race flag to keep them out of release builds).
    echo "$pkgspec" | sed 's$./pkg/sql/opt/[^ ]*$$g'
    pkgspec=$(echo "$pkgspec" | sed 's$./pkg/sql/opt[^ ]*$$g')
    pkgspec="$pkgspec ./pkg/sql/opt/..."
  fi
  echo "PR #$TC_BUILD_BRANCH has changed packages; running race detector tests on $pkgspec"
fi
tc_end_block "Determine changed packages"

tc_start_block "Compile C dependencies"
# Buffer noisy output and only print it on failure.
run build/builder.sh make -Otarget c-deps GOFLAGS=-race &> artifacts/race-c-build.log || (cat artifacts/race-c-build.log && false)
rm artifacts/race-c-build.log
tc_end_block "Compile C dependencies"

maybe_stress "stressrace"

# Expect the timeout to come from the TC environment.
TESTTIMEOUT=${TESTTIMEOUT:-45m}

for pkg in $pkgspec; do
  tc_start_block "Run ${pkg} under race detector"
  run_json_test build/builder.sh env \
    COCKROACH_LOGIC_TESTS_SKIP=true \
    GOMAXPROCS=8 \
    stdbuf -oL -eL \
    make testrace \
    GOTESTFLAGS=-json \
    PKG="$pkg" \
    TESTTIMEOUT="${TESTTIMEOUT}" \
    TESTFLAGS="-v $TESTFLAGS" \
    ENABLE_LIBROACH_ASSERTIONS=1
  tc_end_block "Run ${pkg} under race detector"
done
