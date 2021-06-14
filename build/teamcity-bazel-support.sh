BAZEL_IMAGE=cockroachdb/bazel:20210528-135020

# Call `run_bazel $NAME_OF_SCRIPT` to start an appropriately-configured Docker
# container with the `cockroachdb/bazel` image running the given script.
#
# Set the TEAMCITY_BAZEL_SUPPORT_LINT variable for lints -- this will mount the
# workspace as writeable in the Docker container, will avoid setting up the
# artifacts directory, etc.
run_bazel() {
    if [ -z "${root:-}" ]
    then
        echo '$root is not set; please source teamcity-support.sh'
        exit 1
    fi

    # Set up volumes.
    vols="--volume /home/agent/.bazelcache:/root/.cache/bazel"

    workspace_vol="--volume ${root}:/go/src/github.com/cockroachdb/cockroach"
    if [ -z "${TEAMCITY_BAZEL_SUPPORT_LINT:-}" ]
    then
        artifacts_dir=$root/artifacts
        mkdir -p "$artifacts_dir"

        vols="${vols} ${workspace_vol}:ro"
        vols="${vols} --volume ${artifacts_dir}:/artifacts"
    else
        vols="${vols} ${workspace_vol}"
    fi

    docker run -i ${tty-} --rm --init \
        --workdir="/go/src/github.com/cockroachdb/cockroach" \
        ${vols} \
        $BAZEL_IMAGE "$@"
}
