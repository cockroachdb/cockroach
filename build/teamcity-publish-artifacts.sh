#!/usr/bin/env bash

# Any arguments to this script are passed through unmodified to
# ./build/teamcity-publish-s3-binaries.

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/teamcity-publish-s3-binaries.sh "$@"

if [[ "$TC_BUILD_BRANCH" != *alpha* ]] && [ "$TEAMCITY_BUILDCONF_NAME" == 'Publish Releases' ]; then
	image=docker.io/cockroachdb/cockroach

	cp cockroach-linux-2.6.32-gnu-amd64 build/deploy/cockroach
	docker build --tag=$image:{latest,"$TC_BUILD_BRANCH"} build/deploy

	build/builder.sh make TYPE=release-linux-gnu testbuild TAGS=acceptance PKG=./pkg/acceptance
	(cd pkg/acceptance && ./acceptance.test -i $image -b /cockroach/cockroach -nodes 3 -test.v -test.timeout -5m)

	sed "s/<EMAIL>/$DOCKER_EMAIL/;s/<AUTH>/$DOCKER_AUTH/" < build/.dockercfg.in > ~/.dockercfg
	docker push "$image:latest"
	docker push "$image:$TC_BUILD_BRANCH"
fi
