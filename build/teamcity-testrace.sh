#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Maybe stressrace pull request"
build/builder.sh go install ./pkg/cmd/github-pull-request-make
build/builder.sh env BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" TARGET=stressrace github-pull-request-make
tc_end_block "Maybe stressrace pull request"

tc_start_block "Determine changed packages"
if tc_release_branch; then
	pkgspec=./pkg/...
  echo "On release branch ($TC_BUILD_BRANCH), so running testrace on all packages ($pkgspec)"
else
  pkgspec=$(changed_go_pkgs)
	if [[ -z "$pkgspec" ]]; then
		echo "PR #$TC_BUILD_BRANCH has no changed packages; skipping race detector tests"
		exit 0
	fi
	echo "PR #$TC_BUILD_BRANCH has changed packages; running race detector tests on $pkgspec"
fi
tc_end_block "Determine changed packages"

tc_start_block "Compile"
run build/builder.sh make -Otarget gotestdashi GOFLAGS=-race
tc_end_block "Compile"

tc_start_block "Run Go tests under race detector"
run build/builder.sh env \
    COCKROACH_LOGIC_TESTS_SKIP=true \
    make testrace \
    PKG="$pkgspec" \
    TESTTIMEOUT=45m \
    TESTFLAGS='-v' \
    ENABLE_ROCKSDB_ASSERTIONS=1 2>&1 \
	| tee artifacts/testrace.log \
	| go-test-teamcity
tc_end_block "Run Go tests under race detector"
