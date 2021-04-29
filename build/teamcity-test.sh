#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$PWD/artifacts/test
mkdir -p "$TMPDIR"

tc_start_block "Compile C dependencies"
# Buffer noisy output and only print it on failure.
run build/builder.sh make -Otarget c-deps &> artifacts/c-build.log || (cat artifacts/c-build.log && false)
rm artifacts/c-build.log
tc_end_block "Compile C dependencies"

maybe_stress stress

tc_start_block "Run Go tests"

# Many of our tests start many goroutines that do a non-trivial amount
# of work. Here, we limit the concurrent test programs that will be
# run to 80% of the available CPU.
GOTESTFLAGS="-json"
if command -v nproc >/dev/null 2>&1; then
    PROCCOUNT=$(nproc)
    if [[ "$PROCCOUNT" -ge 4 ]]; then
        MAXJOBS=$(echo "$PROCCOUNT*0.8/1" | bc)
        echo "Setting -p $MAXJOBS (cpu count: $PROCCOUNT)"
        GOTESTFLAGS="$GOTESTFLAGS -p $MAXJOBS"
    else
        echo "less than 4 CPUs, not setting -p flag"
    fi
else
    echo "warning: nproc not found, not setting -p flag"
fi

run_json_test build/builder.sh stdbuf -oL -eL make test GOTESTFLAGS="$GOTESTFLAGS" TESTFLAGS="-v"
tc_end_block "Run Go tests"
