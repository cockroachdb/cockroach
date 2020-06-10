#!/usr/bin/env bash

set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

export BUILDER_HIDE_GOPATH_SRC=1

make .buildinfo/tag
build_name="$(cat .buildinfo/tag)"

bucket="${BUCKET-cockroach-prerelease}"

tc_start_block "Tag the release"
git tag "$build_name"
tc_end_block "Tag the release"


tc_start_block "Compile publish-provisional-artifacts"
build/builder.sh go install ./pkg/cmd/publish-provisional-artifacts
tc_end_block "Compile publish-provisional-artifacts"


tc_start_block "Compile and publish binaries and archive artifacts"
build/builder.sh env \
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  TC_BUILD_BRANCH="$build_name" \
  publish-provisional-artifacts -provisional -release -bucket "$bucket"
tc_end_block "Compile and publish binaries and archive artifacts"


tc_start_block "Make and push docker image"
# TODO: get logged into gcloud

# TODO: Settle on the desired repo name
gcr_repository=gcr.io/cockroach-managed-staging/cockroach

# TODO: Where are the binaries deposited locally?
cp cockroach build/deploy/cockroach

docker build --no-cache --tag="${gcr_repository}:${build_name}" build/deploy
docker push "gcr://${gcr_repository}:${build_name}"
tc_end_block "Make and push docker image"


tc_start_block "Push release tag to github.com/cockroachdb/cockroach"
git remote add release-staging git@github.com:cockroachlabs/release-staging.git
git push release-staging "$build_name"
tc_end_block "Push release tag to github.com/cockroachdb/cockroach"


tc_start_block "Tag docker image as latest-build"
docker tag "gcr://${gcr_repository}:${build_name}" "gcr://${gcr_repository}:latest-build"
tc_end_block "Tag docker image as latest-build"
