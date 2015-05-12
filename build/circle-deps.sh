#!/bin/bash

set -ex

if [ "$1" = "docker" ]; then
    cmds=$(grep '^cmd' GLOCKFILE | grep -v glock | awk '{print $2}')

    # Restore previously cached build artifacts.
    time go install github.com/cockroachdb/build-cache
    time build-cache restore . .:race,test ${cmds}
    # Build everything needed by circle-test.sh.
    time go test -race -v -i ./...
    time go install -v ${cmds}
    time make install
    time (cd acceptance; go test -v -c -tags acceptance)
    # Cache the current build artifacts for future builds.
    time build-cache save . .:race,test ${cmds}

    exit 0
fi

gopath0="${GOPATH%%:*}"
cachedir="${gopath0}/pkg/cache"
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

HOME= go get -d -u github.com/cockroachdb/build-cache
HOME= go get -u github.com/robfig/glock
grep -v '^cmd' GLOCKFILE | glock sync -n

# Pretend we're already bootstrapped, so that `make` doesn't go
# through the bootstrap process which would glock sync and run
# build/devbase/deps.sh (unnecessarily).
touch .bootstrap

# Recursively invoke this script inside the builder docker container,
# passing "docker" as the first argument.
$(dirname $0)/builder.sh $0 docker

# Clear the cache of any files that haven't been modified in more than 3 days.
find "${cachedir}" -mmin +4320 -ls -delete
du -sh "${cachedir}"
ls -lh "${cachedir}"
