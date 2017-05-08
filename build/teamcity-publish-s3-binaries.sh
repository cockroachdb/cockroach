#!/usr/bin/env bash

# Any arguments to this script are passed through unmodified to
# ./pkg/cmd/publish-artifacts.

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

echo 'starting release build'
cat .buildinfo/tag || true
cat .buildinfo/rev || true
build/builder.sh git status

build/builder.sh go install ./pkg/cmd/publish-artifacts

echo 'installed release builder'
cat .buildinfo/tag || true
cat .buildinfo/rev || true
build/builder.sh git status

build/builder.sh env \
	AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
	AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
	TC_BUILD_BRANCH="$TC_BUILD_BRANCH" \
	publish-artifacts "$@"

echo 'built and uploaded artifacts'
cat .buildinfo/tag || true
cat .buildinfo/rev || true
build/builder.sh git status
