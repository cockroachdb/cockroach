#!/usr/bin/env bash

set -euo pipefail

image=cockroachdb/builder
version=20210625-235250

function init() {
  docker build --tag="${image}" "$(dirname "${0}")/builder"
}

if [ "${1-}" = "pull" ]; then
  docker pull "${image}:${version}"
  exit 0
fi

if [ "${1-}" = "init" ]; then
  init
  exit 0
fi

if [ "${1-}" = "push" ]; then
  init
  tag=$(date +%Y%m%d-%H%M%S)
  docker tag "${image}" "${image}:${tag}"
  docker push "${image}:${tag}"
  exit 0
fi

if [ "${1-}" = "version" ]; then
  echo "${version}"
  exit 0
fi

cached_volume_mode=
delegated_volume_mode=
if [ "$(uname)" = "Darwin" ]; then
  # This boosts filesystem performance on macOS at the cost of some consistency
  # guarantees that are usually unnecessary in development.
  # For details: https://docs.docker.com/docker-for-mac/osxfs-caching/
  delegated_volume_mode=:delegated
  cached_volume_mode=:cached
fi

# We don't want this to emit -json output.
GOPATH=$(GOFLAGS=; go env GOPATH)
gopath0=${GOPATH%%:*}
gocache=${GOCACHEPATH-$gopath0}

if [ -t 0 ]; then
  tty=--tty
fi

# Absolute path to the toplevel cockroach directory.
cockroach_toplevel=$(dirname "$(cd "$(dirname "${0}")"; pwd)")

# Ensure the artifact sub-directory always exists and redirect
# temporary file creation to it, so that CI always picks up temp files
# (including stray log files).
mkdir -p "${cockroach_toplevel}"/artifacts
export TMPDIR=$cockroach_toplevel/artifacts

# We'll mount a fresh directory owned by the invoking user as the container
# user's home directory because various utilities (e.g. bash writing to
# .bash_history) need to be able to write to there.
container_home=/home/roach
host_home=${cockroach_toplevel}/build/builder_home
mkdir -p "${host_home}"

# Since we're mounting both /root and its subdirectories in our container,
# Docker will create the subdirectories on the host side under the directory
# that we're mounting as /root, as the root user. This creates problems for CI
# processes trying to clean up the working directory, so we create them here
# as the invoking user to avoid root-owned paths.
#
# Note: this only happens on Linux. On Docker for Mac, the directories are
# still created, but they're owned by the invoking user already. See
# https://github.com/docker/docker/issues/26051.
mkdir -p "${host_home}"/.yarn-cache

# Run our build container with a set of volumes mounted that will
# allow the container to store persistent build data on the host
# computer.
#
# This script supports both circleci and development hosts, so it must
# support cases where the architecture inside the container is
# different from that outside the container. We can map /src/ directly
# into the container because it is architecture-independent. We then
# map certain subdirectories of ${GOPATH}/pkg into both ${GOPATH}/pkg
# and ${GOROOT}/pkg. The ${GOROOT} mapping is needed so they can be
# used to cache builds of the standard library. /bin/ is mapped
# separately to avoid clobbering the host's binaries. Note that the
# path used for the /bin/ mapping is also used in the defaultBinary
# function of localcluster.go.
#
# We always map the cockroach source directory that contains this script into
# the container's $GOPATH/src. By default, we also mount the host's $GOPATH/src
# directory to the container's $GOPATH/src. That behavior can be turned off by
# setting BUILDER_HIDE_GOPATH_SRC to 1, which results in only the cockroach
# source code (and its vendored dependencies) being available within the
# container. This setting is useful to prevent missing vendored dependencies
# from being accidentally resolved to the hosts's copy of those dependencies.

# Ensure that all directories to which the container must be able to write are
# created as the invoking user. Docker would otherwise create them when
# mounting, but that would deny write access to the invoking user since docker
# runs as root.

vols=
# It would be cool to interact with Docker from inside the builder, but the
# socket is owned by root, and our unpriviledged user can't access it. If we
# could make this work, we could run our acceptance tests from inside the
# builder, which would be cleaner and simpler than what we do now (which is to
# build static binaries in the container and then run them on the host).
#
# vols="${vols} --volume=/var/run/docker.sock:/var/run/docker.sock"
vols="${vols} --volume=${host_home}:${container_home}${cached_volume_mode}"

mkdir -p "${HOME}"/.yarn-cache
vols="${vols} --volume=${HOME}/.yarn-cache:${container_home}/.yarn-cache${cached_volume_mode}"

# If we're running in an environment that's using git alternates, like TeamCity,
# we must mount the path to the real git objects for git to work in the container.
# alternates_file=${cockroach_toplevel}/.git/objects/info/alternates
# if test -e "${alternates_file}"; then
#   alternates_path=$(cat "${alternates_file}")
#   vols="${vols} --volume=${alternates_path}:${alternates_path}${cached_volume_mode}"
# fi

teamcity_alternates="/home/agent/system/git/"
if test -d "${teamcity_alternates}"; then
    vols="${vols} --volume=${teamcity_alternates}:${teamcity_alternates}${cached_volume_mode}"
fi

if [ "${BUILDER_HIDE_GOPATH_SRC:-}" != "1" ]; then
  vols="${vols} --volume=${gopath0}/src:/go/src${cached_volume_mode}"
fi
vols="${vols} --volume=${cockroach_toplevel}:/go/src/github.com/cockroachdb/cockroach${cached_volume_mode}"

# If ${cockroach_toplevel}/bin doesn't exist on the host, Docker creates it as
# root unless it already exists. Create it first as the invoking user.
# (This is a bug in the Docker daemon that only occurs when bind-mounted volumes
# are nested, as they are here.)
mkdir -p "${cockroach_toplevel}"/bin{.docker_amd64,}
vols="${vols} --volume=${cockroach_toplevel}/bin.docker_amd64:/go/src/github.com/cockroachdb/cockroach/bin${delegated_volume_mode}"
mkdir -p "${cockroach_toplevel}"/lib{.docker_amd64,}
vols="${vols} --volume=${cockroach_toplevel}/lib.docker_amd64:/go/src/github.com/cockroachdb/cockroach/lib${delegated_volume_mode}"

mkdir -p "${gocache}"/docker/bin
vols="${vols} --volume=${gocache}/docker/bin:/go/bin${delegated_volume_mode}"
mkdir -p "${gocache}"/docker/native
vols="${vols} --volume=${gocache}/docker/native:/go/native${delegated_volume_mode}"
mkdir -p "${gocache}"/docker/pkg
vols="${vols} --volume=${gocache}/docker/pkg:/go/pkg${delegated_volume_mode}"

# Attempt to run in the container with the same UID/GID as we have on the host,
# as this results in the correct permissions on files created in the shared
# volumes. This isn't always possible, however, as IDs less than 100 are
# reserved by Debian, and IDs in the low 100s are dynamically assigned to
# various system users and groups. To be safe, if we see a UID/GID less than
# 500, promote it to 501. This is notably necessary on macOS Lion and later,
# where administrator accounts are created with a GID of 20. This solution is
# not foolproof, but it works well in practice.
uid=$(id -u)
gid=$(id -g)
[ "$uid" -lt 500 ] && uid=501
[ "$gid" -lt 500 ] && gid=$uid

# temporary disable immediate exit on failure, so that we could catch
# the status of "docker run"
set +e

# -i causes some commands (including `git diff`) to attempt to use
# a pager, so we override $PAGER to disable.

# When running in CI (and generally in automated processes) we want
# to avoid the "troubleshooting mode" of datadriven tests, which
# echoes on stderr everything it does (not just failures).
# The caller can override the env var on the way in; this default
# is only used if the env var is not set already.
DATADRIVEN_QUIET_LOG=${DATADRIVEN_QUIET_LOG-true}

# shellcheck disable=SC2086
docker run --init --privileged -i ${tty-} --rm \
  -u "$uid:$gid" \
  ${vols} \
  --workdir="/go/src/github.com/cockroachdb/cockroach" \
  --env="TMPDIR=/go/src/github.com/cockroachdb/cockroach/artifacts" \
  --env="PAGER=cat" \
  --env="GOTRACEBACK=${GOTRACEBACK-all}" \
  --env="TZ=America/New_York" \
  --env="DATADRIVEN_QUIET_LOG=${DATADRIVEN_QUIET_LOG}" \
  --env=COCKROACH_BUILDER_CCACHE \
  --env=COCKROACH_BUILDER_CCACHE_MAXSIZE \
  "${image}:${version}" "$@"

# Build container needs to have at least 4GB of RAM available to compile the project
# successfully, which is not true in some cases (i.e. Docker for MacOS by default).
# Check if it might be the case if "docker run" failed
res=$?
set -e

if test $res -ne 0 -a \( ${1-x} = "make" -o ${1-x} = "xmkrelease" \) ; then
   ram=$(docker run -i --rm \
         -u "$uid:$gid" \
         "${image}:${version}" awk '/MemTotal/{print $2}' /proc/meminfo)

   if test $ram -lt 4000000; then
      ram_gb=`printf "%.2f" $(echo $ram/1024/1024 | bc -l)`

      ram_message="Note: if the build seems to terminate for unclear reasons, \
                note that your container limits RAM to ${ram_gb}GB. This may be \
                the cause of the failure. Try increasing the limit to 4GB or above."

      echo $ram_message >&2   # be mindful of printing to stderr, not stdout
   fi
fi

exit $res
