#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/release/teamcity-support.sh"

gar_repository="us-east1-docker.pkg.dev/crl-ci-images/cockroach/bazel"
docker_login_gcr "$gar_repository" "$IMAGE_BUILDER_GOOGLE_CREDENTIALS"

TAG=$(date +%Y%m%d-%H%M%S)
docker buildx create --name "builder-$TAG" --use
docker buildx build --push --platform linux/amd64,linux/arm64 -t "$gar_repository:$TAG" -t "$gar_repository:latest-do-not-use" build/bazelbuilder

if [[ "$open_pr_on_success" == "true" ]]; then
    # Trigger "Open New Bazel Builder Image PR".
    curl -u "$TC_API_USER:$TC_API_PASSWORD" -X POST \
      "https://$TC_SERVER_URL/app/rest/buildQueue" \
      -H 'Accept: application/json' \
      -H 'Content-Type: application/xml' \
      -H "Host: $TC_SERVER_URL" \
      -d '<build branchName="master">
      <buildType id="Internal_Cockroach_Build_Ci_OpenNewBazelBuilderImagePr"/>
       <properties>
            <property name="env.BRANCH" value="'"bazel-builder-update-$TAG"'"/>
            <property name="env.VERSION" value="'"$gar_repository:$TAG"'"/>
        </properties>
    </build>'
else
	echo "No-op - opening a PR was not requested"
fi
