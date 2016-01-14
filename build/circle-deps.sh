#!/bin/bash

set -eux

function is_shard() {
  test $(($1 % $CIRCLE_NODE_TOTAL)) -eq $CIRCLE_NODE_INDEX
}

# This is mildly tricky: This script runs itself recursively. The
# first time it is run it does not take the if-branch below and
# executes on the host computer. As a final step it uses the
# builder.sh script to run itself inside of docker passing "docker" as
# the argument causing the commands in the if-branch to be executed
# within the docker container.
if [ "${1-}" = "docker" ]; then
  # Be careful to keep the dependencies built for each shard in sync
  # with the dependencies used by each shard in circle-test.sh.

  # Note the ordering here and later in this script of the shard tests
  # is a bit odd. We perform the tests in this order to maintain the
  # same build order as the pre-parallel deps script.

  if is_shard 2; then
    # Symlink the cache into the source tree if they don't exist.
    # In circleci they never do, but this script can also be run
    # locally where they might.
    for i in bower_components node_modules typings; do
      if ! [ -e ui/$i ]; then
        # Create the cache directory to avoid errors on `ln` below.
        # `/uicache` is `~/uicache` on the host computer, which is cached
        # by circle-ci.
        mkdir -p /uicache/$i
        ln -s /uicache/$i ui/$i
      fi
    done

    time make -C ui npm.installed   || rm -rf ui/node_modules/{*,.bin} && make -C ui npm.installed
    time make -C ui bower.installed || rm -rf ui/bower_components/*    && make -C ui bower.installed
    time make -C ui tsd.installed   || rm -rf ui/typings/*             && make -C ui tsd.installed

    # Fix permissions on the ui/typings directory and subdirectories
    # (they lack the execute permission for group/other).
    find /uicache/typings -type d | xargs chmod 0755

    cmds=$(grep '^cmd' GLOCKFILE | grep -v glock | awk '{print $2}')
    time go install -v ${cmds}
  fi

  if is_shard 1; then
    time go test -race -v -i ./...
    time go install -v github.com/tebeka/go2xunit
  fi

  if is_shard 0; then
    time make install GOFLAGS=-v
    time go test -v -i ./...
    time go test -v -c -tags acceptance ./acceptance
    # Avoid code rot.
    time go build ./gossip/simulation/...
  fi

  exit 0
fi

shard1_done="${HOME}/shard1"
shard2_done="${HOME}/shard2"

function notify() {
  if is_shard 2; then
    time ssh node0 touch "${shard2_done}"
  fi

  if is_shard 1; then
    time ssh node0 touch "${shard1_done}"
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

# The tag for the cockroachdb/builder image. If the image is changed
# (for example, adding "npm"), a new image should be pushed using
# "build/builder.sh push" and the new tag value placed here.
tag="20151210-134003"

if ! docker images | grep -q "${tag}"; then
  # If there's a base image cached, load it. A click on CircleCI's "Clear
  # Cache" will make sure we start with a clean slate.
  builder="${builder_dir}/builder.${tag}.tar"
  find "${builder_dir}" -not -path "${builder}" -type f -delete  
  if [[ ! -e "${builder}" ]]; then
    time docker pull "cockroachdb/builder:${tag}"
    docker tag -f "cockroachdb/builder:${tag}" "cockroachdb/builder:latest"
    time docker save "cockroachdb/builder:${tag}" > "${builder}"
  else
    time docker load -i "${builder}"
    docker tag -f "cockroachdb/builder:${tag}" "cockroachdb/builder:latest"
  fi
fi

HOME="" go get -u github.com/robfig/glock
grep -v '^cmd' GLOCKFILE | glock sync -n

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
    dir="${HOME}/uicache"
    time ssh node0 sudo chown -R ubuntu.ubuntu "${dir}"
    time rsync -a --delete "${dir}/" node0:"${dir}"

    cmds=$(grep '^cmd' GLOCKFILE | grep -v glock | awk '{print $2}' | awk -F/ '{print $NF}')
    time ssh node0 mkdir -p "${gopath0}/bin/linux_amd64"
    time ssh node0 sudo chown ubuntu.ubuntu "${gopath0}/bin/linux_amd64"
    for cmd in ${cmds}; do
      path="${gopath0}/bin/linux_amd64/${cmd}"
      time rsync "${path}" node0:"${path}"
    done
  fi
  notify
fi

if is_shard 1; then
  # We might already be on shard 0 if we're running without
  # parallelism. Avoid deleting our build output in that case.
  if ! is_shard 0; then
    dir="${gopath0}/pkg/linux_amd64_race"
    time ssh node0 sudo chown -R ubuntu.ubuntu "${dir}"
    time rsync -a --delete "${dir}/" node0:"${dir}"
  fi
  notify
fi

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
    dir="${gopath0}/pkg/linux_amd64"
    time sudo chown -R ubuntu.ubuntu "${dir}"
    time rsync -au node${node}:"${dir}/" "${dir}"
  fi
fi
