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

  if is_shard 0; then
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

    time make -C ui npm.installed   || rm -rf ui/node_modules/*     && make -C ui npm.installed
    time make -C ui bower.installed || rm -rf ui/bower_components/* && make -C ui bower.installed
    time make -C ui tsd.installed   || rm -rf ui/typings/*          && make -C ui tsd.installed

    cmds=$(grep '^cmd' GLOCKFILE | grep -v glock | awk '{print $2}')
    time go install -v ${cmds}
  fi

  if is_shard 1; then
    time make install GOFLAGS=-v
    time go test -v -i ./...
    time go test -v -c -tags acceptance ./acceptance
    # Avoid code rot.
    time go build ./gossip/simulation/...
  fi

  if is_shard 2; then
    time go test -race -v -i ./...
  fi

  exit 0
fi

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
