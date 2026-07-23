#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -xeuo pipefail

# We use a docker image mirror to avoid pulling from 3rd party repos, which sometimes have reliability issues.
# See https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/3462594561/Docker+image+sync for the details.
BASE_IMAGE="us-east1-docker.pkg.dev/crl-docker-sync/docker-io/library/ubuntu:focal"
BAZEL_IMAGE="us-east1-docker.pkg.dev/crl-ci-images/cockroach/bazel:latest-do-not-use"

docker pull $BASE_IMAGE && docker pull $BAZEL_IMAGE
LATEST_BASE_IMAGE_CREATE_DT="$(docker inspect $BASE_IMAGE -f '{{.Created}}')"
LATEST_BAZEL_IMAGE_CREATE_DT="$(docker inspect $BAZEL_IMAGE -f '{{.Created}}')"

if [[ $LATEST_BASE_IMAGE_CREATE_DT < $LATEST_BAZEL_IMAGE_CREATE_DT ]]; then
    echo "Base image is up to date. No-op."
    exit 0
fi

# Trigger "Build and Push Bazel Builder Image" in TeamCity and pass option to open PR if successful.
curl -u "$TC_API_USER:$TC_API_PASSWORD" -X POST \
  "https://$TC_SERVER_URL/app/rest/buildQueue" \
  -H 'Accept: application/json' \
  -H 'Content-Type: application/xml' \
  -H "Host: $TC_SERVER_URL" \
  -d '<build branchName="master">
  <buildType id="Internal_Cockroach_Build_Ci_BuildAndPushBazelBuilderImage"/>
   <properties>
        <property name="env.open_pr_on_success" value="true"/>
    </properties>
</build>'
