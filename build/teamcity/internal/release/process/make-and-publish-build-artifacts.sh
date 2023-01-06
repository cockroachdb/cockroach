#!/usr/bin/env bash

set -euo pipefail

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
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$build_name -e gcs_credentials -e gcs_bucket=$gcs_bucket" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/publish-provisional-artifacts
BAZEL_BIN=$(bazel info bazel-bin --config ci)
export google_credentials="$gcs_credentials"
source "build/teamcity-support.sh"  # For log_into_gcloud
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
$BAZEL_BIN/pkg/cmd/publish-provisional-artifacts/publish-provisional-artifacts_/publish-provisional-artifacts -provisional -release --gcs-bucket="$gcs_bucket"
EOF
tc_end_block "Compile and publish artifacts"

tc_start_block "Make and push docker image"
configure_docker_creds
docker_login_with_google

# TODO: update publish-provisional-artifacts with option to leave one or more cockroach binaries in the local filesystem
# NB: tar usually stops reading as soon as it sees an empty block but that makes
# curl unhappy, so passing `i` will cause it to read to the end.
curl -f -s -S -o- "${download_prefix}/cockroach-${build_name}.linux-amd64.tgz" | tar ixfz - --strip-components 1
cp cockroach lib/libgeos.so lib/libgeos_c.so build/deploy
cp -r licenses build/deploy/

docker rmi registry.access.redhat.com/ubi8/ubi-minimal || true
docker build --pull --platform linux/amd64 --no-cache --tag="${gcr_repository}:${build_name}" build/deploy
docker push "${gcr_repository}:${build_name}"
tc_end_block "Make and push docker image"

# Make finding the tag name easy.
cat << EOF


Build ID: ${build_name}


EOF
