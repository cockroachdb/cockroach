#!/usr/bin/env bash
set -euxo pipefail
COCKROACH_PATH="${GOPATH}/src/github.com/cockroachdb/cockroach"
source "${COCKROACH_PATH}/build/jepsen-common.sh"

tc SuiteStarted ''

exitcode=0
for test in "${tests[@]}"; do
        # Capitalize the test name.
    caps=$(echo "${test:0:1}"|tr a-z A-Z)${test:1}

    tc SuiteStarted "$caps"

    for nemesis in "${nemeses[@]}"; do
        # Produce a test name.

        # Reduce "--nemesis X --nemesis2 Y" to "X+Y"
        nemname=${nemesis// /}
        nemname=${nemname#--nemesis}
        nemname=${nemname//--nemesis2/+}
        nemname=${nemname//--nemesis/+}

        # Generate a proper test name.
        testname=$caps/$nemname

        if [ "$testname" = "Bank-multitable/start-kill-2" ]; then
            # This test is flaky with mysterious ssh errors.
            # https://github.com/cockroachdb/cockroach/issues/20493
            continue
        fi

        if ! $BASH "${COCKROACH_PATH}/build/teamcity-jepsen-run-one.sh" "$testname" "$test" "$nemesis"; then
            exitcode=1
        fi
    done

    tc SuiteFinished "$caps"
done

tc SuiteFinished ''
