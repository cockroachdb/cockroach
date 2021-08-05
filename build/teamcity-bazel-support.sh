# FYI: You can run `./dev builder` to run this Docker image. :)
# `dev` depends on this variable! Don't change the name or format unless you
# also update `dev` accordingly.
BAZEL_IMAGE=cockroachdb/bazel:20210729-154657

# Call `run_bazel $NAME_OF_SCRIPT` to start an appropriately-configured Docker
# container with the `cockroachdb/bazel` image running the given script.
#
# Set the TEAMCITY_BAZEL_SUPPORT_GENERATE variable for scripts that generate
# files in the workspace -- this will mount the workspace as writeable in the
# Docker container, will avoid setting up the artifacts directory, etc.
run_bazel() {
    if [ -z "${root:-}" ]
    then
        echo '$root is not set; please source teamcity-support.sh'
        exit 1
    fi

    # Set up volumes.
    vols="--volume /home/agent/.bazelcache:/root/.cache/bazel"
    # TeamCity uses git alternates, so make sure we mount the path to the real
    # git objects.
    teamcity_alternates="/home/agent/system/git"
    vols="${vols} --volume ${teamcity_alternates}:${teamcity_alternates}:ro"

    workspace_vol="--volume ${root}:/go/src/github.com/cockroachdb/cockroach"
    if [ -z "${TEAMCITY_BAZEL_SUPPORT_GENERATE:-}" ]
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
