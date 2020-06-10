#!/usr/bin/env bash

set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

export BUILDER_HIDE_GOPATH_SRC=1


build_name=$(echo ${NAME} | grep -E -o '^v[0-9]+\.[0-9]+\.[0-9]+$')

if [[ -z "$build_name" ]] ; then
    echo 'Invalid NAME. Must be of the format "v20.1.1".'
    exit 1
fi

release_branch=$(echo ${build_name} | grep -E -o '^v[0-9]+\.[0-9]+')


tc_start_block "Tag the release"
git tag "$build_name"
tc_end_block "Tag the release"


tc_start_block "Compile publish-provisional-artifacts"
build/builder.sh go install ./pkg/cmd/publish-provisional-artifacts
tc_end_block "Compile publish-provisional-artifacts"


tc_start_block "Compile and publish release binaries and archive artifacts"
build/builder.sh env \
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  TC_BUILD_BRANCH="$build_name" \
  publish-provisional-artifacts -release
tc_end_block "Compile and publish release binaries and archive artifacts"


tc_start_block "Make and push docker images"
# Work around headless d-bus problem by forcing docker to use
# plain-text credentials for dockerhub.
# https://github.com/docker/docker-credential-helpers/issues/105#issuecomment-420480401
mkdir -p ~/.docker
cat << EOF > ~/.docker/config.json
{
  "credsStore" : "",
  "auths": {
    "https://index.docker.io/v1/" : {

    }
  }
}
EOF

echo $DOCKER_AUTH | docker login --username $DOCKER_EMAIL --password-stdin
# sed "s/<EMAIL>/$DOCKER_EMAIL/;s/<AUTH>/$DOCKER_AUTH/" < build/.dockercfg.in > ~/.dockercfg

# TODO: get logged into gcloud

#image_repository=docker.io/cockroachdb/cockroach
dockerhub_repository=docker.io/cockroachdb/cockroach-test

# TODO: Settle on the desired repo name
#gcr _repository=gcr.io/cockroach-managed-staging/cockroach
gcr_repository=gcr.io/cockroach-managed-staging/cockroach-test

# TODO: Where are the binaries deposited locally?
cp cockroach build/deploy/cockroach
docker build --no-cache --tag=${dockerhub_repository}:{"$build_name",latest,latest-"${release_branch}"} --tag=${gcr_repository}:${build_name} build/deploy

docker push "${dockerhub_repository}:${build_name}"
docker push "gcr://${gcr_repository}:${build_name}"
tc_end_block "Make and push docker images"


tc_start_block "Push release tag to github.com/cockroachdb/cockroach"
git remote add cockroachdb_cockroach git@github.com:cockroachdb/cockroach.git
git push cockroachdb_cockroach "$build_name"
tc_end_block "Push release tag to github.com/cockroachdb/cockroach"


# Get the latest major release version. Result looks like "v20.1".
# https://github.com/cockroachdb/dev-inf/issues/41
version_to_tag_as_latest=$(curl -s -S "https://registry.hub.docker.com/v2/repositories/cockroachdb/cockroach/tags/?Page_size=100" | \
         grep -E -o '"name":\s*"[^"]*"' | grep -v '"latest"' | \
         awk -F\" '{print $4}' | \
         sort -r --version-sort | \
         head -n1 | grep -E -o '^v[0-9]+\.[0-9]+')


tc_start_block "Publish binaries and archive as latest"
if [[ "$build_name" == ${version_to_tag_as_latest}.* ]]; then
  #TODO: implement me!
fi
tc_end_block "Publish binaries and archive as latest"


tc_start_block "Publish binaries and archive as latest"
# Only push the "latest" for our most recent release branch.
# https://github.com/cockroachdb/cockroach/issues/41067
if [[ "$build_name" == ${version_to_tag_as_latest}.* ]]; then
  build/builder.sh env \
    AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    TC_BUILD_BRANCH="$build_name" \
    publish-provisional-artifacts -bless -release
fi
tc_end_block "Publish binaries and archive as latest"


tc_start_block "Tag docker image as latest-RELEASE_BRANCH"
# Only push the "latest" tag for our most recent release branch.
# https://github.com/cockroachdb/cockroach/issues/41067
if [[ -z "$PRE_RELEASE" ]]; then
  docker push "${dockerhub_repository}:latest-${release_branch}"
fi
tc_end_block "Tag docker image as latest"


tc_start_block "Tag docker image as latest"
# Only push the "latest" tag for our most recent release branch.
# https://github.com/cockroachdb/cockroach/issues/41067
if [[ "$build_name" == ${version_to_tag_as_latest}.* && -z "$PRE_RELEASE" ]]; then
  docker push "${dockerhub_repository}:latest"
fi
tc_end_block "Tag docker image as latest"
