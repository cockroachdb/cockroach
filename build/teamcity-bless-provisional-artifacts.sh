#!/usr/bin/env bash

set -euxo pipefail

#TODO(dan): move all this into publish-provisional-artifacts

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

release_version=$(echo $TC_BUILD_BRANCH | sed -e 's/provisional_[[:digit:]]*_//')
curl -f -s -S -o- https://binaries.cockroachdb.com/cockroach-${release_version}.linux-amd64.tgz | tar xfz - --strip-components 1

image=docker.io/cockroachdb/cockroach-unstable
if [[ "$release_version" != *-* && -z "$FORCE_PUSH_TO_COCKROACH_UNSTABLE" ]]; then
  image=docker.io/cockroachdb/cockroach
fi

cp cockroach build/deploy/cockroach
docker build --no-cache --tag=$image:{latest,"$release_version"} build/deploy

# Only push the "latest" tag for our most recent release branch.
# We assume that VERSION_TO_TAG_AS_LATEST looks like "19.2", and then
# perform a glob match.
# https://github.com/cockroachdb/cockroach/issues/41067
if [[ "$release_version" == v$VERSION_TO_TAG_AS_LATEST.* && -z "$FORCE_PUSH_TO_COCKROACH_UNSTABLE" ]]; then
  docker push "$image:latest"
  build/teamcity-bless-provisional-binaries.sh -release
fi
docker push "$image:$release_version"
