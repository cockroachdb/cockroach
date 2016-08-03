#!/bin/bash

set -eux

CIRCLE_NODE_INDEX="${CIRCLE_NODE_INDEX-0}"
CIRCLE_NODE_TOTAL="${CIRCLE_NODE_TOTAL-1}"

function is_shard() {
  test $(($1 % $CIRCLE_NODE_TOTAL)) -eq $CIRCLE_NODE_INDEX
}

function fetch_docker() {
  local user="$1"
  local repo="${2}"
  local tag="${3}"
  local name="${user}/${repo}"
  local ref="${name}:${tag}"
  if ! docker images --format {{.Repository}}:{{.Tag}} | grep -q "${ref}"; then
    # If we have a saved image, load it.
    imgcache="${builder_dir}/${user}.${repo}.tar"
    if [[ -e "${imgcache}" ]]; then
      time docker load -i "${imgcache}"
    fi

    # If we still don't have the tag we want: pull it and save it.
    if ! docker images --format {{.Repository}}:{{.Tag}} | grep -q "${ref}"; then
      time docker pull "${ref}"
      time docker save -o "${imgcache}" "${ref}"
    fi
  fi
}

# This is mildly tricky: This script runs itself recursively. The
# first time it is run it does not take the if-branch below and
# executes on the host computer. As a final step it uses the
# builder.sh script to run itself inside of docker passing "docker" as
# the argument causing the commands in the if-branch to be executed
# within the docker container.
if [ "${1-}" = "docker" ]; then
  # Fetch glock without -u: glock will update itself (and once
  # glock has updated itself, "go get -u" no longer works because
  # the clone's head is detached).
  go get github.com/robfig/glock
  glock sync -n < GLOCKFILE

  # Be careful to keep the dependencies built for each shard in sync
  # with the dependencies used by each shard in circle-test.sh.

  # Note the ordering here and later in this script of the shard tests
  # is a bit odd. We perform the tests in this order to maintain the
  # same build order as the pre-parallel deps script.

  if is_shard 2; then
    make -C ui jspm.installed

    # TODO(pmattis): This works around the problem seen in
    # https://github.com/cockroachdb/cockroach/issues/4013 where
    # certain checks and code generation tools rely on compiled
    # packages. In particular, `stringer` definitely relies on
    # compiled packages for imports and it appears `go vet` is
    # similar. Would be nice to find a different solution, but simply
    # removing the out of date packages does not fix the problem (it
    # causes `stringer` to complain).
    time make build
  fi

  if is_shard 1; then
    time go test -race -v -i ./...
    # We need go2xunit on both shards that run go tests.
    time go install -v github.com/tebeka/go2xunit
  fi

  if is_shard 0; then
    time make install
    time go test -v -i ./...
    time go test -v -c -tags acceptance ./acceptance
  fi

  exit 0
fi

shard1_done="${HOME}/shard1"
shard2_done="${HOME}/shard2"

function notify() {
  if is_shard 1; then
    if ! is_shard 0; then
      time ssh node0 touch "${shard1_done}"
    else
      touch "${shard1_done}"
    fi
  fi

  if is_shard 2; then
    if ! is_shard 0; then
      time ssh node0 touch "${shard2_done}"
    else
      touch "${shard2_done}"
    fi
  fi
}

# Notify shard 0 that shard's 1 and 2 are done if they exit for any
# reason. This prevents shard 0 from spinning forever.
trap "notify" EXIT

# `~/builder` is cached by circle-ci.
builder_dir=~/builder
mkdir -p "${builder_dir}"
du -sh "${builder_dir}"
ls -lah "${builder_dir}"

fetch_docker "cockroachdb" "builder" $($(dirname $0)/builder.sh version)

if is_shard 0; then
  # See the comment in build/builder.sh about this awk line.
  postgresTestTag=$(awk -F\" '/postgresTestTag *=/ {print $2}' \
                      $(dirname $0)/../acceptance/util_test.go)
  if [ -z "${postgresTestTag}" ]; then
    echo "unable to determine postgres-test tag"
    exit 1
  fi
  # Dockerfile at: https://github.com/cockroachdb/postgres-test
  fetch_docker "cockroachdb" "postgres-test" "${postgresTestTag}"
fi

# Recursively invoke this script inside the builder docker container,
# passing "docker" as the first argument.
$(dirname $0)/builder.sh $0 docker

# According to
# https://discuss.circleci.com/t/cache-save-restore-algorithm/759, the
# cache is only collected from container (shard) 0. We work around
# this limitation by copying the build output from shards 1 and 2 over
# to shard 0. On shard 0 we wait for this remote data to be copied.

gopath0="${GOPATH%%:*}"

if is_shard 2; then
  # We might already be on shard 0 if we're running without
  # parallelism. Avoid deleting our build output in that case.
  if ! is_shard 0; then
    for dir in "${HOME}/.jspm" "${HOME}/.npm"; do
      time rsync -a --delete "${dir}/" node0:"${dir}"
    done

    cmds=$(grep '^cmd ' GLOCKFILE | grep -v glock | awk '{print $2}' | awk -F/ '{print $NF}')
    path="${gopath0}/bin/docker_amd64"
    time ssh node0 mkdir -p "$path"
    (time cd "$path" && rsync ${cmds} node0:"${path}")
  fi
fi

if is_shard 1; then
  # We might already be on shard 0 if we're running without
  # parallelism. Avoid deleting our build output in that case.
  if ! is_shard 0; then
    dir="${gopath0}/pkg/docker_amd64_race"
    time rsync -a --delete "${dir}/" node0:"${dir}"
  fi
fi

# Need to notify before exit in case we're running without
# parallelism.
notify

if is_shard 0; then
  set +x
  start=$(date +%s)
  while :; do
    if [ -e "${shard1_done}" -a -e "${shard2_done}" ]; then
      break
    fi
    sleep 1
  done
  echo "waited $(($(date +%s) - ${start})) secs"

  # Sync the go packages from the node which built the commands. We
  # have to do this as a pull instead of as a push because node 0 is
  # building an overlapping set of packages.
  if ! is_shard 2; then
    node=$((2 % $CIRCLE_NODE_TOTAL))
    dir="${gopath0}/pkg/docker_amd64"
    time rsync -au node${node}:"${dir}/" "${dir}"
  fi
fi
