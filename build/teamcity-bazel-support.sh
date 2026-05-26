# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

if [ -z "${root:-}" ]
then
    echo '$root is not set; please source teamcity-support.sh'
    exit 1
fi

# FYI: You can run `./dev builder` to run this Docker image. :)
BAZEL_IMAGE=$(cat $root/build/.bazelbuilderversion)

# Capture all "COCKROACH_*" environment variables, generating a string
# in the format:
#
# -e COCKROACH_VAR1 -e COCKROACH_VAR2 ...
#
# This can be passed to the `docker` call in run_bazel, allowing
# engineers to set COCKROACH_* variables via TeamCity and have those
# set in the environment where `cockroach` runs.
#
# NB: `|| true` stops the command from returning 1 if there are no
# COCKROACH_* variables; that would cause builds that use `pipefail`
# to fail.
DOCKER_EXPORT_COCKROACH_VARS=$(env | grep '^COCKROACH_' | cut -d= -f1 | sed -e 's/\(.*\)/-e \1/' | tr '\n' ' ') || true

# Call `run_bazel $NAME_OF_SCRIPT` to start an appropriately-configured Docker
# container with the `us-east1-docker.pkg.dev/crl-ci-images/cockroach/bazel`
# image running the given script.
# BAZEL_SUPPORT_EXTRA_DOCKER_ARGS will be passed on to `docker run` unchanged.
run_bazel() {
    if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
        run_bazel_github "$@"
        return
    fi
    # Set up volumes.
    # TeamCity uses git alternates, so make sure we mount the path to the real
    # git objects.
    teamcity_alternates="/home/agent/system/git"
    vols="--volume ${teamcity_alternates}:${teamcity_alternates}:ro"
    vols="${vols} --volume ${TEAMCITY_BUILD_PROPERTIES_FILE}:${TEAMCITY_BUILD_PROPERTIES_FILE}:ro"
    artifacts_dir=$root/artifacts
    mkdir -p "$artifacts_dir"
    vols="${vols} --volume ${artifacts_dir}:/artifacts"
    cache=/home/agent/.bzlhome24
    mkdir -p $cache
    vols="${vols} --volume ${root}:/go/src/github.com/cockroachdb/cockroach"
    vols="${vols} --volume ${cache}:/home/roach"

    exit_status=0
    docker run -i ${tty-} --rm --init \
        -u "$(id -u):$(id -g)" \
        --workdir="/go/src/github.com/cockroachdb/cockroach" \
	${DOCKER_EXPORT_COCKROACH_VARS} \
	${BAZEL_SUPPORT_EXTRA_DOCKER_ARGS:+$BAZEL_SUPPORT_EXTRA_DOCKER_ARGS} \
        ${vols} \
        $BAZEL_IMAGE "$@" || exit_status=$?
    rm -rf _bazel
    return $exit_status
}

# GitHub Actions sibling of run_bazel. Kept separate so the TC body of
# run_bazel above stays identical to its pre-GHA-migration form, which
# lets TC-side fixes backport cleanly to release branches that don't
# carry the GHA migration.
run_bazel_github() {
    artifacts_dir=$root/artifacts
    mkdir -p "$artifacts_dir"
    vols="--volume ${artifacts_dir}:/artifacts"
    vols="${vols} --volume ${root}:/go/src/github.com/cockroachdb/cockroach"
    cache="${RUNNER_TEMP:-/tmp}/.bzlhome"
    mkdir -p "$cache"
    vols="${vols} --volume ${cache}:/home/roach"
    if [[ -n "${GOOGLE_GHA_CREDS_PATH:-}" && -f "${GOOGLE_GHA_CREDS_PATH}" ]]; then
        vols="${vols} --volume ${GOOGLE_GHA_CREDS_PATH}:${GOOGLE_GHA_CREDS_PATH}:ro"
        BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="${BAZEL_SUPPORT_EXTRA_DOCKER_ARGS:-} -e CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=${GOOGLE_GHA_CREDS_PATH} -e GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_GHA_CREDS_PATH}"
    fi

    exit_status=0
    docker run -i ${tty-} --rm --init \
        -u "$(id -u):$(id -g)" \
        --workdir="/go/src/github.com/cockroachdb/cockroach" \
	${DOCKER_EXPORT_COCKROACH_VARS} \
	${BAZEL_SUPPORT_EXTRA_DOCKER_ARGS:+$BAZEL_SUPPORT_EXTRA_DOCKER_ARGS} \
        ${vols} \
        $BAZEL_IMAGE "$@" || exit_status=$?
    rm -rf _bazel
    return $exit_status
}

# local copy of _tc_build_branch from teamcity-support.sh to avoid imports.
_tc_build_branch() {
    echo "${TC_BUILD_BRANCH#refs/heads/}"
}

# local copy of tc_release_branch from teamcity-support.sh to avoid imports.
_tc_release_branch() {
  branch=$(_tc_build_branch)
  [[ "$branch" == master || "$branch" == release-* || "$branch" == provisional_* ]]
}
