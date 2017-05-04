#!/usr/bin/env bash

# Arguments to this script are passed through to ./pkg/cmd/release-upload.

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

echo 'starting release build'
cat .buildinfo/tag || true
cat .buildinfo/rev || true
build/builder.sh git status

build/builder.sh go install ./pkg/cmd/release-upload

echo 'installed release builder'
cat .buildinfo/tag || true
cat .buildinfo/rev || true
build/builder.sh git status

build/builder.sh env \
	AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
	AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
	TC_BUILD_BRANCH="$TC_BUILD_BRANCH" \
	release-upload "$@"

echo 'built and uploaded artifacts'
cat .buildinfo/tag || true
cat .buildinfo/rev || true
build/builder.sh git status

if [ "$TC_BUILD_BRANCH" != master ]; then
	image=docker.io/cockroachdb/cockroach

	cp cockroach-linux-2.6.32-gnu-amd64 build/deploy/cockroach
	docker build --tag=$image:{latest,"$TC_BUILD_BRANCH"} build/deploy

	build/builder.sh make TYPE=release-linux-gnu testbuild TAGS=acceptance PKG=./pkg/acceptance
	(cd pkg/acceptance && ./acceptance.test -i $image -b /cockroach/cockroach -nodes 3 -test.v -test.timeout -5m)

	sed "s/<EMAIL>/$DOCKER_EMAIL/;s/<AUTH>/$DOCKER_AUTH/" < build/.dockercfg.in > ~/.dockercfg
	docker push "$image:latest"
	docker push "$image:$TC_BUILD_BRANCH"
fi
