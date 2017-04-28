#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh go install ./pkg/cmd/release
build/builder.sh env TC_BUILD_BRANCH="$TC_BUILD_BRANCH" release

if [ "$TC_BUILD_BRANCH" != master ]; then
	image=docker.io/cockroachdb/cockroach

	cp cockroach-linux-2.6.32-gnu-amd64 build/deploy/cockroach
	docker build --tag=$image:{latest,"$TC_BUILD_BRANCH"} build/deploy

	build/builder.sh make TYPE=release-linux-gnu testbuild TAGS=acceptance PKG=./pkg/acceptance
	cd pkg/acceptance
	time ./acceptance.test -i $image -b /cockroach/cockroach -nodes 3 -test.v -test.timeout -5m

	docker login --email "$DOCKER_EMAIL" --password "$DOCKER_AUTH"
	docker push $image:{latest,"$TC_BUILD_BRANCH"}
fi
