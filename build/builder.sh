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

uicache_dir="uicache"

# Absolute path to the toplevel cockroach directory.
cockroach_toplevel="$(dirname $(cd $(dirname $0); pwd))"

# Run our build container with a set of volumes mounted that will
# allow the container to store persistent build data on the host
# computer.
#
# This script supports both circleci and development hosts, so it must
# support cases where the architecture inside the container is
# different from that outside the container. We map /src/ directly
# into the container because it is architecture-independent, and /pkg/
# because every subdirectory is tagged with its architecture. We also
# map certain subdirectories of ${GOPATH}/pkg into ${GOROOT}/pkg so
# they can be used to cache race and netgo builds of the standard
# library. /bin/ is mapped separately to avoid clobbering the host's
# binaries. Note that the path used for the /bin/ mapping is also used
# in the defaultBinary function of localcluster.go.
#
# -i causes some commands (including `git diff`) to attempt to use
# a pager, so we override $PAGER to disable.
docker run -i ${tty-} ${rm} \
  --volume="${gopath0}/src:/go/src" \
  --volume="${gopath0}/pkg:/go/pkg" \
  --volume="${gopath0}/pkg/linux_amd64_race:/usr/src/go/pkg/linux_amd64_race" \
  --volume="${gopath0}/bin/linux_amd64:/go/bin" \
  --volume="${HOME}/${uicache_dir}:/${uicache_dir}" \
  --volume="${cockroach_toplevel}:/go/src/github.com/cockroachdb/cockroach" \
  --workdir="/go/src/github.com/cockroachdb/cockroach" \
  --env="PAGER=cat" \
  --env="TSD_GITHUB_TOKEN=${TSD_GITHUB_TOKEN-}" \
  --env="CIRCLE_NODE_INDEX=${CIRCLE_NODE_INDEX-0}" \
  --env="CIRCLE_NODE_TOTAL=${CIRCLE_NODE_TOTAL-1}" \
  "${image}:${version}" "$@"
