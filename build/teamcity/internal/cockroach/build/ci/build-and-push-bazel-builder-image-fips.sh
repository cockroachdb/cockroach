#!/usr/bin/env bash
set -xeuo pipefail

PKGDIR=build/bazelbuilder/packages
mkdir $PKGDIR
cd $PKGDIR
# repackage FIPS-related packages. This operation has to happen on a FIPS-enabled host.
for pkg in openssl libssl1.1 libssl1.1-hmac kcapi-tools libkcapi1; do
    dpkg-repack "$pkg"
done
cd -

TAG=$(cut -d: -f2 build/.bazelbuilderversion)
DOCKER_BUILDKIT=1 docker build -t "cockroachdb/bazel-fips:$TAG" \
    --build-arg FROM_IMAGE="cockroachdb/bazel:$TAG" \
    -f build/bazelbuilder/Dockerfile.fips build/bazelbuilder
docker push "cockroachdb/bazel-fips:$TAG"
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
            <property name="env.VERSION" value="'"cockroachdb/bazel-fips:$TAG"'"/>
        </properties>
    </build>'
else
	echo "No-op - opening a PR was not requested"
fi
