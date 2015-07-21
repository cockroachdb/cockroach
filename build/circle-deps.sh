#!/bin/bash

set -ex

# This is mildly tricky: This script runs itself recursively. The
# first time it is run it does not take the if-branch below and
# executes on the host computer. As a final step it uses the
# builder.sh script to run itself inside of docker passing "docker" as
# the argument causing the commands in the if-branch to be executed
# within the docker container.
if [ "${1:-}" = "docker" ]; then
    cmds=$(grep '^cmd' GLOCKFILE | grep -v glock | awk '{print $2}')

    # Pretend we're already bootstrapped, so that `make` doesn't go
    # through the bootstrap process which would glock sync and run
    # build/devbase/deps.sh (unnecessarily).
    touch .bootstrap

    # Restore previously cached build artifacts.
    time go install github.com/cockroachdb/build-cache
    time build-cache restore . .:race,test ${cmds}
    # Build everything needed by circle-test.sh.
    time go test -race -v -i ./...
    time go install -v ${cmds}
    time make GITHOOKS= install
    time (cd acceptance; go test -v -c -tags acceptance)
    # Cache the current build artifacts for future builds.
    time build-cache save . .:race,test ${cmds}

    exit 0
fi

gopath0="${GOPATH%%:*}"
cachedir="${gopath0}/pkg/cache"

# The tag for the cockroachdb/builder image. If the image is changed
# (for example, adding "npm"), a new image should be pushed using
# "build/builder.sh push" and the new tag value placed here.
tag="20150719-133944"

mkdir -p "${cachedir}"
du -sh "${cachedir}"
ls -lh "${cachedir}"

if ! docker images | grep -q "${tag}"; then
    # If there's a base image cached, load it. A click on CircleCI's "Clear
    # Cache" will make sure we start with a clean slate.
    rm -f "$(ls ${cachedir}/builder*.tar 2>/dev/null | grep -v ${tag})"
    builder="${cachedir}/builder.${tag}.tar"
    if [[ ! -e "${builder}" ]]; then
	time docker pull "cockroachdb/builder:${tag}"
	docker tag -f "cockroachdb/builder:${tag}" "cockroachdb/builder:latest"
	time docker save "cockroachdb/builder:${tag}" > "${builder}"
    else
	time docker load -i "${builder}"
	docker tag -f "cockroachdb/builder:${tag}" "cockroachdb/builder:latest"
    fi
fi

# TODO(pmattis): This script differs from build/devbase/deps.sh. It
# would be nice to re-unify the two. The problematic part is that this
# script performs "git" work in the host environment and "go" work
# inside the docker container. The "deps.sh" script doesn't know about
# the separation between the two environments.

HOME="" go get -d -u github.com/cockroachdb/build-cache
HOME="" go get -u github.com/robfig/glock
grep -v '^cmd' GLOCKFILE | glock sync -n

# Recursively invoke this script inside the builder docker container,
# passing "docker" as the first argument.
$(dirname $0)/builder.sh $0 docker

# Clear the cache of any files that haven't been modified in more than 3 days.
find "${cachedir}" -mmin +4320 -ls -delete
du -sh "${cachedir}"
ls -lh "${cachedir}"
