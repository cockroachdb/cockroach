#!/usr/bin/env bash

set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh make .buildinfo/tag
build_name="${NAME-$(cat .buildinfo/tag)}"
release_branch="$(echo "$build_name" | grep -Eo "^v[0-9]+\.[0-9]+")"

# TODO: What's the bucket name?
#bucket="${BUCKET-cockroach-prerelease}"
bucket="${BUCKET-james-cockroach-test}"


tc_start_block "Tag the release"
git tag "$build_name"
tc_end_block "Tag the release"


tc_start_block "Compile publish-provisional-artifacts"
build/builder.sh go install ./pkg/cmd/publish-provisional-artifacts
tc_end_block "Compile publish-provisional-artifacts"


tc_start_block "Compile and publish S3 artifacts"
build/builder.sh env \
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  TC_BUILD_BRANCH="$build_name" \
  publish-provisional-artifacts -provisional -release -bucket "$bucket"
tc_end_block "Compile and publish S3 artifacts"


tc_start_block "Make and push docker image"
log_into_gcloud
configure_docker_creds

# TODO: Settle on the desired repo name
#gcr_repository=gcr.io/cockroach-managed-staging/cockroach
gcr_repository=gcr.io/cockroach-managed-staging/shrug

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
# Only tag the image as "latest-vX.Y-build" if the tag is on a release branch
if [[ -n "${release_branch}" ]] ; then
  gcloud container images add-tag "${gcr_repository}:${build_name}" "${gcr_repository}:latest-${release_branch}-build"
fi
tc_end_block "Tag docker image as latest-build"
