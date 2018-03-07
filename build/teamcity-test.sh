#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Compile roachtest"
run build/builder.sh make bin/roachtest
tc_end_block "Compile roachtest"

tc_start_block "Compile roachprod"
run build/builder.sh go get -u -v github.com/cockroachdb/roachprod
tc_end_block "Compile roachprod"

tc_start_block "Compile CockroachDB"
# run build/builder.sh make build
# Use this instead of the `make build` above for faster debugging iteration.
run curl -L https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.LATEST -o cockroach-linux-2.6.32-gnu-amd64
run chmod +x cockroach-linux-2.6.32-gnu-amd64
tc_end_block "Compile CockroachDB"

tc_start_block "Compile workload"
run build/builder.sh make generate
run build/builder.sh make bin/workload
tc_end_block "Compile workload"

tc_start_block "Run roachtest"
run build/builder.sh env \
    ./bin/roachtest run kv0 --local --workload=bin/workload --cockroach=cockroach-linux-2.6.32-gnu-amd64
    | go-test-teamcity
tc_end_block "Run roachtest"

# tc_start_block "Maybe stress pull request"
# run build/builder.sh go install ./pkg/cmd/github-pull-request-make
# run build/builder.sh env BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" TARGET=stress github-pull-request-make
# tc_end_block "Maybe stress pull request"

# tc_start_block "Compile"
# run build/builder.sh make -Otarget gotestdashi
# tc_end_block "Compile"

# tc_start_block "Run Go tests"
# run build/builder.sh env TZ=America/New_York make test TESTFLAGS='-v' 2>&1 \
# 	| tee artifacts/test.log \
# 	| go-test-teamcity
# tc_end_block "Run Go tests"

# tc_start_block "Run C++ tests"
# run build/builder.sh make check-libroach
# tc_end_block "Run C++ tests"
