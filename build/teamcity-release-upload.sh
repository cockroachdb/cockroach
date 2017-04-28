#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh go install ./pkg/cmd/release
build/builder.sh env TC_BUILD_BRANCH="$TC_BUILD_BRANCH" release

if [ "$TC_BUILD_BRANCH" != master ]; then
	cp cockroach-linux-2.6.32-gnu-amd64 build/deploy/cockroach
	docker build \
		-t  docker.io/cockroachdb/cockroach:latest \
		-t "docker.io/cockroachdb/cockroach:$TC_BUILD_BRANCH" \
		build/deploy

	build/builder.sh make TYPE=release-linux-gnu testbuild TAGS=acceptance PKG=./pkg/acceptance
	cd pkg/acceptance
	time ./acceptance.test -i cockroachdb/cockroach -b /cockroach/cockroach -nodes 3 -test.v -test.timeout -5m

	docker push docker.io/cockroachdb/cockroach
fi
