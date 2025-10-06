#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/release/teamcity-support.sh"

gar_repository="us-east1-docker.pkg.dev/crl-ci-images/cockroach/bazel-fips"
docker_login_gcr "$gar_repository" "$IMAGE_BUILDER_GOOGLE_CREDENTIALS"

BASE_IMAGE="us-east1-docker.pkg.dev/crl-ci-images/cockroach/bazel"
PKGDIR=build/bazelbuilder/packages
mkdir $PKGDIR
cd $PKGDIR
# repackage FIPS-related packages. This operation has to happen on a FIPS-enabled host.
for pkg in openssl libssl1.1 libssl1.1-hmac kcapi-tools libkcapi1; do
    dpkg-repack "$pkg"
done
cd -

TAG=$(cut -d: -f2 build/.bazelbuilderversion)
DOCKER_BUILDKIT=1 docker build -t "$gar_repository:$TAG" \
    --build-arg FROM_IMAGE="$BASE_IMAGE:$TAG" \
    -f build/bazelbuilder/Dockerfile.fips build/bazelbuilder
docker push "$gar_repository:$TAG"
rm -rf $PKGDIR

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
