#!/usr/bin/env bash

set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # for run_bazel

tc_start_block "Variable Setup"

build_name=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)

# On no match, `grep -Eo` returns 1. `|| echo""` makes the script not error.
release_branch="$(echo "$build_name" | grep -Eo "^v[0-9]+\.[0-9]+" || echo"")"
is_custom_build="$(echo "$TC_BUILD_BRANCH" | grep -Eo "^custombuild-" || echo "")"

if [[ -z "${DRY_RUN}" ]] ; then
  gcs_bucket="cockroach-builds-artifacts-prod"
  google_credentials=$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS
  gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
  # Used for docker login for gcloud
  gcr_hostname="us-docker.pkg.dev"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_PROD"
else
  gcs_bucket="cockroach-builds-artifacts-dryrun"
  google_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
  build_name="${build_name}.dryrun"
  gcr_hostname="us.gcr.io"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_DEV"
fi
download_prefix="https://storage.googleapis.com/$gcs_bucket"

cat << EOF

  build_name:      $build_name
  release_branch:  $release_branch
  is_custom_build: $is_custom_build
  gcs_bucket:      $gcs_bucket
  gcr_repository:  $gcr_repository

EOF
tc_end_block "Variable Setup"


# Leaving the tagging part in place to make sure we don't break any scripts
# relying on the tag.
tc_start_block "Tag the release"
git tag "${build_name}"
tc_end_block "Tag the release"

tc_start_block "Compile and publish artifacts"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$build_name -e build_name=$build_name -e gcs_credentials -e gcs_bucket=$gcs_bucket" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/publish-provisional-artifacts
BAZEL_BIN=$(bazel info bazel-bin --config ci)
export google_credentials="$gcs_credentials"
source "build/teamcity-support.sh"  # For log_into_gcloud
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
$BAZEL_BIN/pkg/cmd/publish-provisional-artifacts/publish-provisional-artifacts_/publish-provisional-artifacts -provisional -release --gcs-bucket="$gcs_bucket" --output-directory=artifacts --build-tag-override="$build_name"
EOF
tc_end_block "Compile and publish artifacts"

tc_start_block "Make and push multiarch docker images"
configure_docker_creds
docker_login_with_google

gcr_tag="${gcr_repository}:${build_name}"
declare -a docker_manifest_amends

for platform_name in amd64 arm64; do
  cp --recursive "build/deploy" "build/deploy-${platform_name}"
  tar \
    --directory="build/deploy-${platform_name}" \
    --extract \
    --file="artifacts/cockroach-${build_name}.linux-${platform_name}.tgz" \
    --ungzip \
    --ignore-zeros \
    --strip-components=1
  cp --recursive licenses "build/deploy-${platform_name}"
  # Move the libs where Dockerfile expects them to be
  mv build/deploy-${platform_name}/lib/* build/deploy-${platform_name}/
  rmdir build/deploy-${platform_name}/lib

  build_docker_tag="${gcr_repository}:${platform_name}-${build_name}"
  docker build --no-cache --pull --platform "linux/${platform_name}" --tag="${build_docker_tag}" "build/deploy-${platform_name}"
  docker push "$build_docker_tag"
  docker_manifest_amends+=("--amend" "${build_docker_tag}")
done

docker manifest create "${gcr_tag}" "${docker_manifest_amends[@]}"
docker manifest push "${gcr_tag}"
tc_end_block "Make and push multiarch docker images"


tc_start_block "Make and push FIPS docker image"
gcr_tag_fips="${gcr_repository}:${build_name}-fips"
platform_name=amd64-fips
rm -rf "build/deploy-${platform_name}"
cp --recursive "build/deploy" "build/deploy-${platform_name}"
tar \
  --directory="build/deploy-${platform_name}" \
  --extract \
  --file="artifacts/cockroach-${build_name}.linux-${platform_name}.tgz" \
  --ungzip \
  --ignore-zeros \
  --strip-components=1
cp --recursive licenses "build/deploy-${platform_name}"
# Move the libs where Dockerfile expects them to be
mv build/deploy-${platform_name}/lib/* build/deploy-${platform_name}/
rmdir build/deploy-${platform_name}/lib

docker build --no-cache --pull --platform "linux/amd64" --tag="${gcr_tag_fips}" --build-arg fips_enabled=1 "build/deploy-${platform_name}"
docker push "$gcr_tag_fips"

tc_end_block "Make and push FIPS docker image"



# Make finding the tag name easy.
cat << EOF


Build ID: ${build_name}


EOF
