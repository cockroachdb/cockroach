#!/usr/bin/env bash

# Any arguments to this script are passed through unmodified to
# ./build/teamcity-publish-s3-binaries.

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/teamcity-publish-s3-binaries.sh "$@"

# When publishing a release, build and publish a docker image.
if [ "$TEAMCITY_BUILDCONF_NAME" == 'Publish Releases' ]; then
  # Unstable releases go to a special cockroach-unstable image name, while
  # stable releases go to the official cockroachdb/cockroach image name.
  # According to semver rules, non-final releases can be distinguished
  # by the presence of a hyphen in the version number.
  image=docker.io/cockroachdb/cockroach-unstable
  if [[ "$TC_BUILD_BRANCH" != *-* ]]; then
    image=docker.io/cockroachdb/cockroach
  fi

  cp cockroach-linux-2.6.32-gnu-amd64 build/deploy/cockroach
  docker build --no-cache --tag=$image:{latest,"$TC_BUILD_BRANCH"} build/deploy

  TYPE=release-$(go env GOOS)
  case $TYPE in
    *-linux)
      TYPE+=-gnu
      ;;
  esac

  # For the acceptance tests that run without Docker.
  ln -s cockroach-linux-2.6.32-gnu-amd64 cockroach
  build/builder.sh make TYPE=$TYPE testbuild TAGS=acceptance PKG=./pkg/acceptance
  (cd pkg/acceptance && ./acceptance.test -l ./artifacts -i $image -b /cockroach/cockroach -nodes 4 -test.v -test.timeout -5m) &> ./artifacts/publish-acceptance.log

  sed "s/<EMAIL>/$DOCKER_EMAIL/;s/<AUTH>/$DOCKER_AUTH/" < build/.dockercfg.in > ~/.dockercfg
  docker push "$image:latest"
  docker push "$image:$TC_BUILD_BRANCH"
fi
