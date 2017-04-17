#!/usr/bin/env bash
set -euxo pipefail

# This script provisions a Jepsen controller and 5 nodes, and runs tests
# against them.

COCKROACH_PATH="${GOPATH}/src/github.com/cockroachdb/cockroach"
KEY_NAME="${KEY_NAME-google_compute_engine}"
LOG_DIR="${COCKROACH_PATH}/artifacts"
mkdir -p "${LOG_DIR}"

cd "${COCKROACH_PATH}/cloud/gce/jepsen"

# Generate ssh keys for the controller to talk to the workers.
rm -f controller.id_rsa controller.id_rsa.pub
ssh-keygen -f controller.id_rsa -N ''

function destroy {
  set +e
  echo "Tearing down cluster..."
  terraform destroy --var=key_name="${KEY_NAME}" --force
}
trap destroy EXIT

# Spin up the cluster.
terraform apply --var=key_name="${KEY_NAME}"

controller="$(terraform output controller-ip)"

nemeses=(
    # big-skews disabled since they assume an eth0 interface.
    #"--nemesis big-skews"
    "--nemesis majority-ring"
    "--nemesis start-stop-2"
    "--nemesis start-kill-2"
    #"--nemesis majority-ring --nemesis2 big-skews"
    #"--nemesis big-skews --nemesis2 start-kill-2"
    "--nemesis majority-ring --nemesis2 start-kill-2"
    "--nemesis parts --nemesis2 start-kill-2"
)

tests=(
    "bank"
    "comments"
    "register"
    "monotonic"
    "sets"
    "sequential"
)

testcmd_base="cd jepsen/cockroachdb && ~/lein run test --tarball file:///home/ubuntu/cockroach.tgz --username ubuntu --ssh-private-key ~/.ssh/id_rsa --nodes-file ~/nodes --time-limit 180 --test-count 1 --os ubuntu"

# Don't quit after just one test.
# Can't have -x on when echoing the teamcity status lines or else we'll
# get duplicates.
set +ex
for test in "${tests[@]}"; do
    for nemesis in "${nemeses[@]}"; do
        # We pipe stdout to /dev/null because it's already recorded by Jepsen
        # and placed in the artifacts for us.
        testcmd="${testcmd_base} --test ${test} ${nemesis} > /dev/null"
        echo "##teamcity[testStarted name='${test} ${nemesis}']"
        echo "Testing with args --test ${test} ${nemesis}"

        # Remove spaces from test name to get the artifacts subdirectory
        testname=$(echo "${test}${nemesis}" | sed 's/ //g')
        artifacts_dir="${LOG_DIR}/${testname}"
        mkdir -p "${artifacts_dir}"

        # Run each test over an ssh connection.
        # If this begins to time out frequently, let's do this via nohup and poll.
        #
        # shellcheck disable=SC2029
        if ssh -o "ServerAliveInterval=60" -o "StrictHostKeyChecking no" -i "$HOME/.ssh/${KEY_NAME}" "ubuntu@${controller}" "${testcmd}"; then
            # Test passed. grab just the results file.
            echo "Test passed. Grabbing minimal logs..."
            scp -o "StrictHostKeyChecking no" -ri "$HOME/.ssh/${KEY_NAME}" "ubuntu@${controller}:jepsen/cockroachdb/store/latest/{test.fressian,results.edn,latency-quantiles.png,latency-raw.png,rate.png}" "${artifacts_dir}"
        else
            # Test failed: grab everything.
            echo "Test failed. Grabbing all logs..."
            archive_path="jepsen/cockroachdb/store/failure-logs.tgz"
            # -h causes tar to follow symlinks; needed by the `latest` symlink.
            ssh -o "StrictHostKeyChecking no" -i "$HOME/.ssh/${KEY_NAME}" "ubuntu@${controller}" "tar -chzf ${archive_path} jepsen/cockroachdb/store/latest"
            scp -o "StrictHostKeyChecking no" -ri "$HOME/.ssh/${KEY_NAME}" "ubuntu@${controller}:${archive_path}" "${artifacts_dir}"
            echo "##teamcity[testFailed name='${test} ${nemesis}']"
        fi
        echo "##teamcity[testFinished name='${test} ${nemesis}']"
        echo "##teamcity[publishArtifacts '${LOG_DIR}']"
    done
done
