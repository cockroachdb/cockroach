#!/usr/bin/env bash

set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # for run_bazel

tc_start_block "Variable Setup"
version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)
version_label=$(echo "${version}" | sed -e 's/^v//' | cut -d- -f 1)

if ! echo "${version}" | grep -E -o '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[-.0-9A-Za-z]+)?$'; then
  #                                    ^major           ^minor           ^patch         ^preRelease
  # Matching the version name regex from within the cockroach code except
  # for the `metadata` part at the end because Docker tags don't support
  # `+` in the tag name.
  # https://github.com/cockroachdb/cockroach/blob/4c6864b44b9044874488cfedee3a31e6b23a6790/pkg/util/version/version.go#L75
  echo "Invalid version \"${version}\". Must be of the format \"vMAJOR.MINOR.PATCH(-PRERELEASE)?\"."
  exit 1
fi

if [[ -z "${DRY_RUN}" ]] ; then
  gcs_bucket="cockroach-release-artifacts-staged-prod"
  gcr_credentials="$GCS_CREDENTIALS_PROD"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_PROD"
  gcr_staged_repository="us-docker.pkg.dev/releases-prod/cockroachdb-staged-releases/cockroach"
else
  gcs_bucket="cockroach-release-artifacts-staged-dryrun"
  gcr_credentials="$GCS_CREDENTIALS_DEV"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_DEV"
  gcr_staged_repository="us-docker.pkg.dev/releases-dev-356314/cockroachdb-staged-releases/cockroach"
fi

tc_end_block "Variable Setup"


tc_start_block "Make and publish release artifacts"
# Using publish-provisional-artifacts here is funky. We're directly publishing
# the official binaries, not provisional ones. Legacy naming. To clean up...
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$version -e gcs_credentials -e gcs_bucket=$gcs_bucket" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/publish-provisional-artifacts
BAZEL_BIN=$(bazel info bazel-bin --config ci)
export google_credentials="$gcs_credentials"
source "build/teamcity-support.sh"  # For log_into_gcloud
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
$BAZEL_BIN/pkg/cmd/publish-provisional-artifacts/publish-provisional-artifacts_/publish-provisional-artifacts -provisional -release --gcs-bucket="$gcs_bucket" --output-directory=artifacts
EOF
tc_end_block "Make and publish release artifacts"


tc_start_block "Make and push multiarch docker images"
docker_login_gcr "$gcr_staged_repository" "$gcr_credentials"

declare -a gcr_arch_tags

for platform_name in amd64 arm64; do
  cp --recursive "build/deploy" "build/deploy-${platform_name}"
  tar \
    --directory="build/deploy-${platform_name}" \
    --extract \
    --file="artifacts/cockroach-${version}.linux-${platform_name}.tgz" \
    --ungzip \
    --ignore-zeros \
    --strip-components=1
  cp --recursive licenses "build/deploy-${platform_name}"
  # Move the libs where Dockerfile expects them to be
  mv build/deploy-${platform_name}/lib/* build/deploy-${platform_name}/
  rmdir build/deploy-${platform_name}/lib

  gcr_arch_tag="${gcr_staged_repository}:${platform_name}-${version}"
  gcr_arch_tags+=("$gcr_arch_tag")

  # Tag the arch specific images with only one tag per repository. The manifests will reference the tags.
  docker build \
    --label version="$version_label" \
    --no-cache \
    --pull \
    --platform="linux/${platform_name}" \
    --tag="${gcr_arch_tag}" \
    "build/deploy-${platform_name}"
  docker push "$gcr_arch_tag"
done

gcr_tag="${gcr_staged_repository}:${version}"
docker manifest rm "${gcr_tag}" || :
docker manifest create "${gcr_tag}" "${gcr_arch_tags[@]}"
docker manifest push "${gcr_tag}"

tc_end_block "Make and push multiarch docker images"


tc_start_block "Make and push FIPS docker image"
platform_name=amd64-fips
cp --recursive "build/deploy" "build/deploy-${platform_name}"
tar \
  --directory="build/deploy-${platform_name}" \
  --extract \
  --file="artifacts/cockroach-${version}.linux-${platform_name}.tgz" \
  --ungzip \
  --ignore-zeros \
  --strip-components=1
cp --recursive licenses "build/deploy-${platform_name}"
# Move the libs where Dockerfile expects them to be
mv build/deploy-${platform_name}/lib/* build/deploy-${platform_name}/
rmdir build/deploy-${platform_name}/lib

gcr_tag_fips="${gcr_staged_repository}:${version}-fips"

# Tag the arch specific images with only one tag per repository. The manifests will reference the tags.
docker build \
  --label version="$version_label" \
  --no-cache \
  --pull \
  --platform="linux/amd64" \
  --tag="${gcr_tag_fips}" \
  --build-arg fips_enabled=1 \
  "build/deploy-${platform_name}"
docker push "$gcr_tag_fips"
tc_end_block "Make and push FIPS docker image"


tc_start_block "Verify docker images"
error=0
for platform_name in amd64 arm64; do
    tc_start_block "Verify $gcr_tag on $platform_name"
    if ! verify_docker_image "$gcr_tag" "linux/$platform_name" "$BUILD_VCS_NUMBER" "$version" false; then
      error=1
    fi
    tc_end_block "Verify $gcr_tag on $platform_name"
done

tc_start_block "Verify $gcr_tag_fips"
if ! verify_docker_image "$gcr_tag_fips" "linux/amd64" "$BUILD_VCS_NUMBER" "$version" true; then
  error=1
fi
tc_end_block "Verify $gcr_tag_fips"

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi
tc_end_block "Verify docker images"
