#!/usr/bin/env bash
set -euxo pipefail
COCKROACH_PATH="${GOPATH}/src/github.com/cockroachdb/cockroach"
source "${COCKROACH_PATH}/build/jepsen-common.sh"

testName=${1:?test label not specified}
test=${2:?Jepsen test name not specified}
nemesis=${3:?Jepsen nemesis flag(s) not specified}

tc Started "$testName"

# The test's log file will go to a sub-dir named after the test. Make it.
artifacts_dir=$(echo "$testName"|tr / _)
mkdir -p "${artifacts_dir}"

# What is the controller again?
controller=$(terraform output controller-ip)

# Prepare the command to run the test. Note that --concurrency must be
# a multiple of 10 and some tests require a minimum of 20.
testcmd="cd jepsen/cockroachdb && set -eo pipefail && \
 stdbuf -oL -eL \
 ~/lein run test \
   --tarball file:///home/ubuntu/cockroach.tgz \
   --username ubuntu \
   --ssh-private-key ~/.ssh/id_rsa \
   --nodes-file ~/nodes \
   --os ubuntu \
   --time-limit 300 \
   --concurrency 30 \
   --recovery-time 25 \
   --test-count 1 \
   --test ${test} ${nemesis} \
2>&1 | stdbuf -oL tee invoke.log"

exitcode=0

# Save our PID for use with pkill's "parent PID" filter below.
# Without this we sometimes get warnings about other ssh processes
# we are unable to kill (or worse, could stomp on another run).
SCRIPT_PID=$$

# Although we run tests of 6 minutes each, we use a timeout
# much larger than that; this is because Jepsen for some tests
# (e.g. register) runs a potentially long analysis after the test
# itself has completed, before determining whether the test has
# succeeded or note.
if timeout 20m ssh "${SSH_OPTIONS[@]}" "ubuntu@${controller}" "${testcmd}" \
        | (set +x; i=1; IFS='
';
           # The following loop displays a TC message every 10 seconds
           # with an excerpt from the jepsen log.
           prevsecs=0
           while true; do
               # Fail if no jepsen logging message within 60 seconds.
               # Note that jepsen sleeps for the --recovery-time
               # parameter above at the end of the test, so this
               # timeout must be greater than that value.
               # Additionally, jepsen uses some hard-coded 30s timeouts
               # internally, so setting this too low may interfere with
               # jepsen's own error reporting.
               read -t 60 x
               status=$?
               if [ $status -gt 128 ]; then
                   progress "Jepsen test was silent for too long, aborting"
                   # timeout: try to get any running jvm to log its stack traces
                   ssh "${SSH_OPTIONS[@]}" "ubuntu@${controller}" pkill -QUIT java
                   sleep 10
                   # kill ssh to abort the test.
                   pkill -P $SCRIPT_PID ssh
                   exit $status
               elif [ $status -ne 0 ]; then
                   progress "Test finished with $i log lines"
                   break
               fi
               secs=$(date +%s);
               if [ $secs -gt $(($prevsecs+10)) ]; then
                   prevsecs=$secs
                   echo "... $x ..."
                   progress "Test still running, $i log lines"
               fi
               i=$(($i+1))
           done; exit 0); then

    # Test passed. grab just the results file.
    progress "Test passed. Grabbing minimal logs..."
    scp "${SSH_OPTIONS[@]}" -C -r \
        "ubuntu@${controller}:jepsen/cockroachdb/store/latest/{test.fressian,results.edn,latency-quantiles.png,latency-raw.png,rate.png}" \
        "${artifacts_dir}"

else
    progress "Test failed: exit code $?. Grabbing artifacts from controller..."
    exitcode=1

    # Show the last few lines from the Jepsen run into the build log.
    ssh "${SSH_OPTIONS[@]}" "ubuntu@${controller}" "tail -n 100 jepsen/cockroachdb/invoke.log" >&2 || echo "Failed to extract the last lines from invoke.log." >&2

    progress Creating archive from controller output
    # Now grab all the artifacts.
    # -h causes tar to follow symlinks; needed by the `latest` symlink.
    ssh "${SSH_OPTIONS[@]}" "ubuntu@${controller}" "tar -chj --ignore-failed-read -f- jepsen/cockroachdb/store/latest jepsen/cockroachdb/invoke.log /var/log/" >"${artifacts_dir}"/failure-logs.tbz || echo "Failed to copy the files." >&2

    progress Resetting latest run for next test
    # Reset the link for the next test run.
    ssh "${SSH_OPTIONS[@]}" "ubuntu@${controller}" "rm -f jepsen/cockroachdb/store/latest" || echo "Failed to remove the latest alias." >&2

    tc Failed "$testName"
fi

tc Finished "$testName"

# For debugging
echo "##teamcity[publishArtifacts '${LOG_DIR}']"

exit $exitcode
