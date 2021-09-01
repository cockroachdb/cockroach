# FYI: You can run `./dev builder` to run this Docker image. :)
# `dev` depends on this variable! Don't change the name or format unless you
# also update `dev` accordingly.
BAZEL_IMAGE=cockroachdb/bazel:20210831-144443

# Call `run_bazel $NAME_OF_SCRIPT` to start an appropriately-configured Docker
# container with the `cockroachdb/bazel` image running the given script.
run_bazel() {
    if [ -z "${root:-}" ]
    then
        echo '$root is not set; please source teamcity-support.sh'
        exit 1
    fi

    # Set up volumes.
    # TeamCity uses git alternates, so make sure we mount the path to the real
    # git objects.
    teamcity_alternates="/home/agent/system/git"
    vols="--volume ${teamcity_alternates}:${teamcity_alternates}:ro"
    artifacts_dir=$root/artifacts
    mkdir -p "$artifacts_dir"
    vols="${vols} --volume ${artifacts_dir}:/artifacts"
    cache=/home/agent/.bzlhome
    mkdir -p $cache
    vols="${vols} --volume ${root}:/go/src/github.com/cockroachdb/cockroach"
    vols="${vols} --volume ${cache}:/home/roach"

    docker run -i ${tty-} --rm --init \
        -u "$(id -u):$(id -g)" \
        --workdir="/go/src/github.com/cockroachdb/cockroach" \
        ${vols} \
        $BAZEL_IMAGE "$@"
}
