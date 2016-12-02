#!/usr/bin/env bash

set -euo pipefail

image="cockroachdb/builder"

# Grab the builder tag from the acceptance tests. We're looking for a
# variable named builderTag, splitting the line on double quotes (")
# and taking the second component.
version=$(awk -F\" '/builderTag *=/ {print $2}' \
            "$(dirname "${0}")"/../pkg/acceptance/cluster/localcluster.go)
if [ -z "${version}" ]; then
  echo "unable to determine builder tag"
  exit 1
fi

function init() {
  docker build --tag="${image}" "$(dirname "${0}")"
}

if [ "${1-}" = "pull" ]; then
  docker pull "${image}"
  exit 0
fi

if [ "${1-}" = "init" ]; then
  init
  exit 0
fi

if [ "${1-}" = "push" ]; then
  init
  tag="$(date +%Y%m%d-%H%M%S)"
  docker tag "${image}" "${image}:${tag}"
  docker push "${image}"
  exit 0
fi

if [ "${1-}" = "version" ]; then
  echo "${version}"
  exit 0
fi

gopath0="${GOPATH%%:*}"

if [ -t 0 ]; then
  tty="--tty"
fi

# Absolute path to the toplevel cockroach directory.
cockroach_toplevel="$(dirname "$(cd "$(dirname "${0}")"; pwd)")"

# Make a fake passwd file for the invoking user.
#
# This setup is so that files created from inside the container in a mounted
# volume end up being owned by the invoking user and not by root.
# We'll mount a fresh directory owned by the invoking user as /root inside the
# container because the container needs a $HOME (without one the default is /)
# and because various utilities (e.g. bash writing to .bash_history) need to be
# able to write to there.
container_home="/root"
host_home="${cockroach_toplevel}/build/builder_home"
passwd_file="${host_home}/passwd"
username=$(id -un)
uid_gid="$(id -u):$(id -g)"
mkdir -p "${host_home}"
echo "${username}:x:${uid_gid}::${container_home}:/bin/bash" > "${passwd_file}"

# Ensure that all directories to which the container must be able to write are
# created as the invoking user. Docker would otherwise create them when
# mounting, but that would deny write access to the invoking user since docker
# runs as root.
mkdir -p "${HOME}"/.{jspm,yarn-cache} "${gopath0}"/pkg/docker_amd64{,_race} "${gopath0}/bin/docker_amd64"

# Since we're mounting both /root and its subdirectories in our container,
# Docker will create the subdirectories on the host side under the directory
# that we're mounting as /root, as the root user. This creates problems for CI
# processes trying to clean up the working directory, so we create them here
# as the invoking user to avoid root-owned paths.
#
# Note: this only happens on Linux. On Docker for Mac, the directories are
# still created, but they're owned by the invoking user already. See
# https://github.com/docker/docker/issues/26051.
mkdir -p "${host_home}"/.{jspm,yarn-cache}

# Run our build container with a set of volumes mounted that will
# allow the container to store persistent build data on the host
# computer.
#
# This script supports both circleci and development hosts, so it must
# support cases where the architecture inside the container is
# different from that outside the container. We map /src/ directly
# into the container because it is architecture-independent. We then
# map certain subdirectories of ${GOPATH}/pkg into both ${GOPATH}/pkg
# and ${GOROOT}/pkg. The ${GOROOT} mapping is needed so they can be
# used to cache builds of the standard library. /bin/ is mapped
# separately to avoid clobbering the host's binaries. Note that the
# path used for the /bin/ mapping is also used in the defaultBinary
# function of localcluster.go.
#
# -i causes some commands (including `git diff`) to attempt to use
# a pager, so we override $PAGER to disable.
vols="--volume=${passwd_file}:/etc/passwd"
vols="${vols} --volume=${host_home}:${container_home}"
vols="${vols} --volume=${gopath0}/src:/go/src"
vols="${vols} --volume=${gopath0}/pkg/docker_amd64:/go/pkg/linux_amd64"
vols="${vols} --volume=${gopath0}/pkg/docker_amd64_race:/go/pkg/linux_amd64_race"
vols="${vols} --volume=${gopath0}/pkg/docker_amd64:/usr/local/go/pkg/linux_amd64"
vols="${vols} --volume=${gopath0}/pkg/docker_amd64_race:/usr/local/go/pkg/linux_amd64_race"
vols="${vols} --volume=${gopath0}/bin/docker_amd64:/go/bin"
vols="${vols} --volume=${HOME}/.jspm:${container_home}/.jspm"
vols="${vols} --volume=${HOME}/.yarn-cache:${container_home}/.yarn-cache"
vols="${vols} --volume=${cockroach_toplevel}:/go/src/github.com/cockroachdb/cockroach"

backtrace_dir="${cockroach_toplevel}/../../cockroachlabs/backtrace"
if test -d "${backtrace_dir}"; then
  vols="${vols} --volume=${backtrace_dir}:/opt/backtrace"
  vols="${vols} --volume=${backtrace_dir}/cockroach.cf:${container_home}/.coroner.cf"
fi

# If we're running in an environment that's using git alternates, like TeamCity,
# we must mount the path to the real git objects for git to work in the container.
alternates_file="${cockroach_toplevel}/.git/objects/info/alternates"
if test -e "${alternates_file}"; then
  alternates_path=$(cat "${alternates_file}")
  vols="${vols} --volume=${alternates_path}:${alternates_path}"
fi

docker run -i ${tty-} --rm \
  -u "${uid_gid}" \
  ${vols} \
  --workdir="/go/src/github.com/cockroachdb/cockroach" \
  --env="PAGER=cat" \
  --env="JSPM_GITHUB_AUTH_TOKEN=${JSPM_GITHUB_AUTH_TOKEN-763c42afb2d31eb7bc150da33402a24d0e081aef}" \
  --env="CIRCLE_NODE_INDEX=${CIRCLE_NODE_INDEX-0}" \
  --env="CIRCLE_NODE_TOTAL=${CIRCLE_NODE_TOTAL-1}" \
  --env="GOOGLE_PROJECT=${GOOGLE_PROJECT-}" \
  --env="GOOGLE_CREDENTIALS=${GOOGLE_CREDENTIALS-}" \
  --env="GOTRACEBACK=${GOTRACEBACK-all}" \
  --env="COVERALLS_TOKEN=${COVERALLS_TOKEN-}" \
  --env="CODECOV_TOKEN=${CODECOV_TOKEN-}" \
  "${image}:${version}" "$@"
