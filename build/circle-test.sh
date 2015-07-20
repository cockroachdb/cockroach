#!/bin/bash

set -e

# Prevent installation of the GITHOOKS for the make invocations
# below. This is needed to prevent undesirable bootstrapping.
export GITHOOKS=

outdir="${TMPDIR}"
if [ "${CIRCLE_ARTIFACTS}" != "" ]; then
    outdir="${CIRCLE_ARTIFACTS}"
fi

builder=$(dirname $0)/builder.sh

# 1. Run "make check" to verify coding guidelines. 
echo "make check"
time ${builder} make check | tee "${outdir}/check.log"
# Early exit on failure.
test ${PIPESTATUS[0]} -eq 0

# 2. Verify that "go generate" was run.
echo "verifying generated files"
time ${builder} /bin/bash -c "(go generate ./... && git ls-files --modified --deleted --others --exclude-standard | diff /dev/null -) || (git add -A && git diff -u HEAD && false)" | tee "${outdir}/generate.log"
# Early exit on failure.
test ${PIPESTATUS[0]} -eq 0

# 3. Run "make test".
match='^panic|^[Gg]oroutine \d+|(read|write) by.*goroutine|DATA RACE'
echo "make test"
time ${builder} make test \
     TESTFLAGS='-v --verbosity=1 --vmodule=monitor=2' | \
     tr -d '\r' | tee "${outdir}/test.log" | \
     grep -E "^\--- (PASS|FAIL)|^(FAIL|ok)|${match}" |
     awk '{print "test:", $0}'
test_status=${PIPESTATUS[0]}
testrace_status=0

# 4. Run "make testrace" only if "make test" succeeded.
if [ ${test_status} -eq 0 ]; then
    echo "make testrace"
    time ${builder} make testrace \
	 TESTFLAGS='-v --verbosity=1 --vmodule=monitor=2' | \
	 tr -d '\r' | tee "${outdir}/testrace.log" | \
	 grep -E "^\--- (PASS|FAIL)|^(FAIL|ok)|${match}" |
	 awk '{print "race:", $0}'
    testrace_status=${PIPESTATUS[0]}
else
    testrace_status=0
fi

# 5a. Translate the log output to xml to integrate with CircleCI
# better.
if [ "${CIRCLE_TEST_REPORTS}" != "" ]; then
    if [ -f "${outdir}/test.log" ]; then
	mkdir -p "${CIRCLE_TEST_REPORTS}/go"
	${builder} go2xunit < "${outdir}/test.log" \
	       > "${CIRCLE_TEST_REPORTS}/go/test.xml"
    fi
    if [ -f "${outdir}/testrace.log" ]; then
	mkdir -p "${CIRCLE_TEST_REPORTS}/race"
	${builder} go2xunit < "${outdir}/testrace.log" \
	       > "${CIRCLE_TEST_REPORTS}/race/testrace.xml"
    fi
fi

# 5b. Generate the slow test output.
find "${outdir}" -name 'test*.log' -type f -exec \
     grep -F ': Test' {} ';' | \
     sed -E 's/(--- PASS: |\(|\))//g' | \
     awk '{ print $2, $1 }' | sort -rn | head -n 10 \
     > "${outdir}/slow.txt"

# 5c. Generate the excerpt output and fail if it is non-empty.
find "${outdir}" -name 'test*.log' -type f -exec \
     grep -B 5 -A 10 -E "^\-{0,3} *FAIL|${match}" {} ';' > "${outdir}/excerpt.txt"
if [ -s "${outdir}/excerpt.txt" -o ${test_status} -ne 0 -o ${testrace_status} -ne 0 ]; then
    if [ ${test_status} -ne 0 ]; then
	echo "FAIL: 'make test' exited with ${test_status}"
    fi
    if [ ${testrace_status} -ne 0 ]; then
	echo "FAIL: 'make testrace' exited with ${testrace_status}"
    fi
    if [ -s "${outdir}/excerpt.txt" ]; then
	echo "FAIL: excerpt.txt is not empty (${outdir}/excerpt.txt)"
	echo
	head -100 "${outdir}/excerpt.txt"
    fi

    if [ "${CIRCLE_BRANCH}" = "master" ] && [ -n "${GITHUB_API_TOKEN}" ]; then
        curl -X POST -H "Authorization: token ${GITHUB_API_TOKEN}" \
             "https://api.github.com/repos/${CIRCLE_PROJECT_USERNAME}/${CIRCLE_PROJECT_REPONAME}/issues" \
             -d "{ \"title\": \"Test failure in CI build ${CIRCLE_BUILD_NUM}\", \"body\": \"The following test appears to have failed:\n\n[#${CIRCLE_BUILD_NUM}](https://circleci.com/gh/${CIRCLE_PROJECT_USERNAME}/${CIRCLE_PROJECT_REPONAME}/${CIRCLE_BUILD_NUM}):\n\n\`\`\`\n$(python -c 'import json,sys; print json.dumps(sys.stdin.read()).strip("\"")' < ${outdir}/excerpt.txt)\n\`\`\`\nPlease assign, take a look and update the issue accordingly.\", \"labels\": [\"test-failure\"] }" > /dev/null
        echo "Found test/race failures in test logs, see excerpt.log and the newly created issue on our issue tracker"
    fi

    exit 1
fi

# 6. Run the acceptance tests (only on Linux). We can run the
# acceptance tests on the Mac's, but circle-deps.sh only built the
# acceptance tests for Linux.
if [ "$(uname)" = "Linux" ]; then
    cd $(dirname $0)/../acceptance
    time ./acceptance.test -test.v -test.timeout -5m
else
    echo "skipping acceptance tests on $(uname): use 'make acceptance' instead"
fi
