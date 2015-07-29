#!/bin/bash

set -euo pipefail

if [ -n "${CIRCLE_ARTIFACTS-}" ]; then
  outdir="${CIRCLE_ARTIFACTS}"
elif [ -n "${TMPDIR-}" ]; then
  outdir="${TMPDIR}"
else
  outdir=""
fi

builder=$(dirname $0)/builder.sh
match='^panic|^[Gg]oroutine \d+|(read|write) by.*goroutine|DATA RACE'

prepare_artifacts() {
  ret=$?

  # Translate the log output to xml to integrate with CircleCI
  # better.
  if [ -n "${CIRCLE_TEST_REPORTS-}" ]; then
    if [ -f "${outdir}/test.log" ]; then
      mkdir -p "${CIRCLE_TEST_REPORTS}/test"
      ${builder} go2xunit < "${outdir}/test.log" \
        > "${CIRCLE_TEST_REPORTS}/test/test.xml"
    fi
    if [ -f "${outdir}/testrace.log" ]; then
      mkdir -p "${CIRCLE_TEST_REPORTS}/race"
      ${builder} go2xunit < "${outdir}/testrace.log" \
        > "${CIRCLE_TEST_REPORTS}/race/testrace.xml"
    fi
  fi

  # Generate the slow test output.
  # We use `tail` below (instead of `sort -r | head` ) to work around
  # the fact that `set -o pipefail` doesn't like it if `sort` exits
  # with a SIGPIPE error code (which piping to `head` can cause).
  find "${outdir}" -name 'test*.log' -type f -exec \
    grep -F ': Test' {} ';' | \
    sed -E 's/(--- PASS: |\(|\))//g' | \
    awk '{ print $2, $1 }' | sort -n | tail -n 10 \
    > "${outdir}/slow.txt"

  # Generate the excerpt output and fail if it is non-empty.
  find "${outdir}" -name 'test*.log' -type f -exec \
    grep -B 5 -A 10 -E "^\-{0,3} *FAIL|${match}" {} ';' > "${outdir}/excerpt.txt"

  if [ -s "${outdir}/excerpt.txt" ]; then
    ret=1

    echo "FAIL: excerpt.txt is not empty (${outdir}/excerpt.txt)"
    echo
    head -n 100 "${outdir}/excerpt.txt"
  fi

  if [ $ret -ne 0 ]; then
    if [ "${CIRCLE_BRANCH-}" = "master" ] && [ -n "${GITHUB_API_TOKEN-}" ]; then
      curl -X POST -H "Authorization: token ${GITHUB_API_TOKEN}" \
        "https://api.github.com/repos/${CIRCLE_PROJECT_USERNAME}/${CIRCLE_PROJECT_REPONAME}/issues" \
        -d "{ \"title\": \"Test failure in CI build ${CIRCLE_BUILD_NUM}\", \"body\": \"The following test appears to have failed:\n\n[#${CIRCLE_BUILD_NUM}](https://circleci.com/gh/${CIRCLE_PROJECT_USERNAME}/${CIRCLE_PROJECT_REPONAME}/${CIRCLE_BUILD_NUM}):\n\n\`\`\`\n$(python -c 'import json,sys; print json.dumps(sys.stdin.read()).strip("\"")' < ${outdir}/excerpt.txt)\n\`\`\`\nPlease assign, take a look and update the issue accordingly.\", \"labels\": [\"test-failure\"] }" > /dev/null
      echo "Found test/race failures in test logs, see excerpt.log and the newly created issue on our issue tracker"
    fi

    exit $ret
  fi
}

trap prepare_artifacts EXIT

# 1. Run "make check" to verify coding guidelines.
echo "make check"
time ${builder} make GITHOOKS= check | tee "${outdir}/check.log"

# 2. Verify that "go generate" was run.
echo "verifying generated files"
time ${builder} /bin/bash -c "(go generate ./... && git ls-files --modified --deleted --others --exclude-standard | diff /dev/null -) || (git add -A && git diff -u HEAD && false)" | tee "${outdir}/generate.log"

# 3. Run "make test".
echo "make test"
time ${builder} make GITHOOKS= test \
  TESTFLAGS='-v --verbosity=1 --vmodule=monitor=2' | \
  tr -d '\r' | tee "${outdir}/test.log" | \
  grep -E "^\--- (PASS|FAIL)|^(FAIL|ok)|${match}" |
  awk '{print "test:", $0}'

# 4. Run "make testrace".
echo "make testrace"
time ${builder} make GITHOOKS= testrace \
  TESTFLAGS='-v --verbosity=1 --vmodule=monitor=2' | \
  tr -d '\r' | tee "${outdir}/testrace.log" | \
  grep -E "^\--- (PASS|FAIL)|^(FAIL|ok)|${match}" |
  awk '{print "race:", $0}'

# 5. Run the acceptance tests (only on Linux). We can run the
# acceptance tests on the Mac's, but circle-deps.sh only built the
# acceptance tests for Linux.
if [ "$(uname)" = "Linux" ]; then
  time $(dirname $0)/../acceptance.test -test.v -test.timeout 5m
else
  echo "skipping acceptance tests on $(uname): use 'make acceptance' instead"
fi
