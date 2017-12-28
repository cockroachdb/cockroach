#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

source "$(dirname "${0}")/teamcity-support.sh"
maybe_ccache

mkdir -p artifacts

build/builder.sh go install ./pkg/cmd/github-pull-request-make

build/builder.sh env \
	BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
	TARGET=stressrace \
	github-pull-request-make

if [[ "${TC_BUILD_BRANCH}" == master ]] || [[ "${TC_BUILD_BRANCH}" == release* ]] ; then
	PKGSPEC="./pkg/..."
else
	git fetch origin master
	PKGSPEC=$(git diff --name-only $(git merge-base HEAD origin/master) | grep "^pkg*.go" || true)
	if [ -z "${PKGSPEC}" ]; then
		echo "No changed packages; skipping race detector tests"
		exit 0
	fi
	PKGSPEC=$(echo "${PKGSPEC}" | xargs -n1 dirname | sort | uniq | sed 's/^/.\//' | paste -sd " " -)
	echo "Running testrace on packages ${PKGSPEC}"
fi

build/builder.sh env \
	COCKROACH_LOGIC_TESTS_SKIP=true \
	make testrace \
	PKG="${PKGSPEC}" \
	TESTFLAGS='-v' \
	2>&1 \
	| tee artifacts/testrace.log \
	| go-test-teamcity
