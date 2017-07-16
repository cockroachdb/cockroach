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

build/builder.sh make generate
build/builder.sh /bin/bash -c '! git status --porcelain | read || (git status; git diff -a 1>&2; exit 1)'

# Run the UI tests and libroach tests. These logically belong in
# teamcity-test.sh, but we do it here to minimize total build time since the
# rest of this script completes much faster than teamcity-test.sh.
build/builder.sh make -C pkg/ui
build/builder.sh make check-libroach
