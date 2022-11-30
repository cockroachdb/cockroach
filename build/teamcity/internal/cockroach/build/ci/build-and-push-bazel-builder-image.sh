#!/usr/bin/env bash
set -xeuo pipefail

TAG=$(date +%Y%m%d-%H%M%S)
docker buildx create --name "builder-$TAG" --use
docker buildx build --push --platform linux/amd64,linux/arm64 -t "cockroachdb/bazel:$TAG" -t "cockroachdb/bazel:latest-do-not-use" build/bazelbuilder

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
            <property name="env.VERSION" value="'"cockroachdb/bazel:$TAG"'"/>
        </properties>
    </build>'
else
	echo "No-op - opening a PR was not requested"
fi
