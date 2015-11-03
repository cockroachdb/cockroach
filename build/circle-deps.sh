#!/bin/bash

set -eux

# This is mildly tricky: This script runs itself recursively. The
# first time it is run it does not take the if-branch below and
# executes on the host computer. As a final step it uses the
# builder.sh script to run itself inside of docker passing "docker" as
# the argument causing the commands in the if-branch to be executed
# within the docker container.
if [ "${1-}" = "docker" ]; then
  cmds=$(grep '^cmd' GLOCKFILE | grep -v glock | awk '{print $2}')

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

  # Restore previously cached build artifacts.
  time go install github.com/cockroachdb/build-cache
  time build-cache restore . .:race,test ${cmds}
  # Build everything needed by circle-test.sh.
  time go test -race -v -i ./...
  time go install -v ${cmds}
  time make install
  time go test -v -c -tags acceptance ./acceptance
  # Cache the current build artifacts for future builds.
  time build-cache save . .:race,test ${cmds}

  exit 0
fi

# `~/buildcache` is cached by circle-ci.
buildcache_dir=~/buildcache

# The tag for the cockroachdb/builder image. If the image is changed
# (for example, adding "npm"), a new image should be pushed using
# "build/builder.sh push" and the new tag value placed here.
tag="20151030-132253"

mkdir -p "${buildcache_dir}"
du -sh "${buildcache_dir}"
ls -lh "${buildcache_dir}"

if ! docker images | grep -q "${tag}"; then
  # If there's a base image cached, load it. A click on CircleCI's "Clear
  # Cache" will make sure we start with a clean slate.
  rm -f "$(ls ${buildcache_dir}/builder*.tar 2>/dev/null | grep -v ${tag})"
  builder="${buildcache_dir}/builder.${tag}.tar"
  if [[ ! -e "${builder}" ]]; then
    time docker pull "cockroachdb/builder:${tag}"
    docker tag -f "cockroachdb/builder:${tag}" "cockroachdb/builder:latest"
    time docker save "cockroachdb/builder:${tag}" > "${builder}"
  else
    time docker load -i "${builder}"
    docker tag -f "cockroachdb/builder:${tag}" "cockroachdb/builder:latest"
  fi
fi

HOME="" go get -d -u github.com/cockroachdb/build-cache
HOME="" go get -u github.com/robfig/glock
grep -v '^cmd' GLOCKFILE | glock sync -n

# Recursively invoke this script inside the builder docker container,
# passing "docker" as the first argument.
$(dirname $0)/builder.sh $0 docker

# Clear the cache of any files that haven't been modified in more than
# 12 hours. Note that the "build-cache restore" call above will
# "touch" any cache files that are used by the current run.
find "${buildcache_dir}" -type f -mmin +720 -ls -delete
du -sh "${buildcache_dir}"
ls -lh "${buildcache_dir}"
