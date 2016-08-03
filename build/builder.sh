#!/bin/bash

set -eu

image="cockroachdb/builder"

# Grab the builder tag from the acceptance tests. We're looking for a
# variable named builderTag, splitting the line on double quotes (")
# and taking the second component.
version=$(awk -F\" '/builderTag *=/ {print $2}' \
            $(dirname $0)/../acceptance/cluster/localcluster.go)
if [ -z "${version}" ]; then
  echo "unable to determine builder tag"
  exit 1
fi

function init() {
  docker build --tag="${image}" "$(dirname $0)"
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

if [ "${CIRCLECI-}" = "true" ]; then
  # HACK: Removal of docker containers fails on circleci with the
  # error: "Driver btrfs failed to remove root filesystem". So if
  # we're running on circleci, just leave the containers around.
  rm=""
else
  rm="--rm"
fi

if [ -t 0 ]; then
  tty="--tty"
fi

# Absolute path to the toplevel cockroach directory.
cockroach_toplevel="$(dirname $(cd $(dirname $0); pwd))"

# Make a fake passwd file for the invoking user.
#
# This setup is so that files created from inside the container in a mounted
# volume end up being owned by the invoking user and not by root.
# We'll mount a fresh directory owned by the invoking user as /root inside
# the container so the container user is able to write there.
BUILD_HOME="/root"
passwd_file="$(mktemp)"
user_group="$(id -u $USER):$(id -g $USER)"
echo "$USER:x:$user_group::${BUILD_HOME}:/bin/bash" > "$passwd_file"

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
vols="--volume=${gopath0}/src:/go/src"
vols="${vols} --volume=${gopath0}/pkg/docker_amd64:/go/pkg/linux_amd64"
vols="${vols} --volume=${gopath0}/pkg/docker_amd64_race:/go/pkg/linux_amd64_race"
vols="${vols} --volume=${gopath0}/pkg/docker_amd64:/usr/local/go/pkg/linux_amd64"
vols="${vols} --volume=${gopath0}/pkg/docker_amd64_race:/usr/local/go/pkg/linux_amd64_race"
vols="${vols} --volume=${gopath0}/bin/docker_amd64:/go/bin"
vols="${vols} --volume=${HOME}/.jspm:${BUILD_HOME}/.jspm"
vols="${vols} --volume=${HOME}/.npm:${BUILD_HOME}/.npm"
vols="${vols} --volume=${cockroach_toplevel}:/go/src/github.com/cockroachdb/cockroach"
vols="${vols} --volume=$(mktemp -d):${BUILD_HOME}"
vols="${vols} --volume=${passwd_file}:/etc/passwd"

backtrace_dir="${cockroach_toplevel}/../../cockroachlabs/backtrace"
if test -d "${backtrace_dir}"; then
  vols="${vols} --volume=${backtrace_dir}:/opt/backtrace"
  vols="${vols} --volume=${backtrace_dir}/cockroach.cf:${BUILD_HOME}/.coroner.cf"
fi

# If we're running in an environment that's using git alternates, like TeamCity,
# we must mount the path to the real git objects for git to work in the container.
alternates_file="${cockroach_toplevel}/.git/objects/info/alternates"
if test -e "${alternates_file}"; then
  alternates_path=$(cat "${alternates_file}")
  vols="${vols} --volume=${alternates_path}:${alternates_path}"
fi

docker run -i ${tty-} ${rm} \
  -u "${user_group}" \
  ${vols} \
  --workdir="/go/src/github.com/cockroachdb/cockroach" \
  --env="HOME=${BUILD_HOME}" \
  --env="PAGER=cat" \
  --env="SKIP_BOOTSTRAP=1" \
  --env="JSPM_GITHUB_AUTH_TOKEN=${JSPM_GITHUB_AUTH_TOKEN-763c42afb2d31eb7bc150da33402a24d0e081aef}" \
  --env="CIRCLE_NODE_INDEX=${CIRCLE_NODE_INDEX-0}" \
  --env="CIRCLE_NODE_TOTAL=${CIRCLE_NODE_TOTAL-1}" \
  --env="GOTRACEBACK=${GOTRACEBACK-all}" \
  "${image}:${version}" "$@"
