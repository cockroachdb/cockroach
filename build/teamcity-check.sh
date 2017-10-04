#!/usr/bin/env bash
set -exuo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

mkdir -p artifacts

build/builder.sh go install ./vendor/github.com/golang/dep/cmd/dep ./pkg/cmd/github-pull-request-make

# Run checkdeps.
build/builder.sh env \
	BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
	TARGET=checkdeps \
	github-pull-request-make

build/builder.sh make lint 2>&1 | tee artifacts/lint.log | go-test-teamcity

# Generation of docs requires Railroad.jar to avoid excessive API requests.
curl https://edge-binaries.cockroachdb.com/tools/Railroad.jar.enc | openssl aes-256-cbc -d -out build/Railroad.jar -k "$RAILROAD_JAR_KEY"

build/builder.sh env COCKROACH_REQUIRE_RAILROAD=true make generate PKG="./pkg/... ./docs/..."
build/builder.sh /bin/bash -c '! git status --porcelain | read || (git status; git diff -a 1>&2; exit 1)'

# Run the UI tests. This logically belongs in teamcity-test.sh, but we do it
# here to minimize total build time since the rest of this script completes
# faster than the non-UI tests.
build/builder.sh make -C pkg/ui
