#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

# dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
# source "$dir/release/teamcity-support.sh"
# source "$dir/teamcity-bazel-support.sh"  # for run_bazel

#tc_start_block "Variable Setup"

platform="${PLATFORM:?PLATFORM must be specified}"
build_name=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)

# On no match, `grep -Eo` returns 1. `|| echo""` makes the script not error.
release_branch="$(echo "$build_name" | grep -Eo "^v[0-9]+\.[0-9]+" || echo"")"
is_customized_build="$(echo "$TC_BUILD_BRANCH" | grep -Eo "^custombuild-" || echo "")"
is_release_build="$(echo "$TC_BUILD_BRANCH" | grep -Eo "^((staging|release|rc)-(v)?[0-9][0-9]\.[0-9](\.0)?).*|master$" || echo "")"

gcs_bucket="bardin-sandbox-10mar25"
# google_credentials=$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS
gcr_repository="us-central1-docker.pkg.dev/bardin-sandbox/bardin-sandbox/cockroach"
# Used for docker login for gcloud
gcr_hostname="us-central1-docker.pkg.dev"
# export the variable to avoid shell escaping
# export gcs_credentials="$GCS_CREDENTIALS_PROD"

cat << EOF

  build_name:          $build_name
  release_branch:      $release_branch
  platform:            $platform
  is_customized_build: $is_customized_build
  gcs_bucket:          $gcs_bucket
  gcr_repository:      $gcr_repository
  is_release_build:    $is_release_build

EOF
#tc_end_block "Variable Setup"


# Leaving the tagging part in place to make sure we don't break any scripts
# relying on the tag.
#tc_start_block "Tag the release"
# git tag "${build_name}"
#tc_end_block "Tag the release"

#tc_start_block "Compile and publish artifacts"
# BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$build_name -e build_name=$build_name -e gcs_credentials -e gcs_bucket=$gcs_bucket -e platform=$platform" bazel << 'EOF'
bazel build //pkg/cmd/publish-provisional-artifacts
BAZEL_BIN=$(bazel info bazel-bin)





cat licenses/THIRD-PARTY-NOTICES.txt > /tmp/THIRD-PARTY-NOTICES.txt.tmp
echo "================================================================================" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp 
echo "Additional licenses" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp 
echo >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
echo "================================================================================" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp 
for f in $(cd licenses && ls -1 * | grep -v THIRD-PARTY-NOTICES.txt | sort ); do
  echo >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo "------------------------------------------------------------------------" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo "Notices from $f" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo "------------------------------------------------------------------------" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  cat "licenses/$f" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
done

tr -d '\r' < /tmp/THIRD-PARTY-NOTICES.txt.tmp > /tmp/THIRD-PARTY-NOTICES.txt

echo "### one"
echo $GOOGLE_APPLICATION_CREDENTIALS
echo "### two"
echo "artifacts/cockroach-${build_name}.${platform}.tgz"

#TC_BUILD_BRANCH=$build_name $BAZEL_BIN/pkg/cmd/publish-provisional-artifacts/publish-provisional-artifacts_/publish-provisional-artifacts -provisional -release --gcs-bucket="$gcs_bucket" --output-directory=artifacts --build-tag-override="$build_name" --platform $platform --third-party-notices-file=/tmp/THIRD-PARTY-NOTICES.txt
# EOF
# tc_end_block "Compile and publish artifacts"

if [[ $platform == "linux-amd64" || $platform == "linux-arm64" || $platform == "linux-amd64-fips" ]]; then
  arch="amd64"
  if [[ $platform == "linux-arm64" ]]; then
    arch="arm64"
  fi

#  tc_start_block "Make and push docker image"
 # docker_login_with_google

  cp -R "build/deploy" "build/deploy-${platform}"
#  tar \
#    --directory="build/deploy-${platform}" \
#    --extract \
#    --file="artifacts/cockroach-${build_name}.${platform}.tgz" \
#    -z \
#    --ignore-zeros \
#    --strip-components=1
  cp LICENSE licenses/THIRD-PARTY-NOTICES.txt "build/deploy-${platform}"
  # Move the libs where Dockerfile expects them to be
#   mv build/deploy-${platform}/lib/* build/deploy-${platform}/
 # rmdir build/deploy-${platform}/lib

  build_docker_tag="${gcr_repository}:${arch}-${build_name}"
  echo $build_docker_tag
  docker build --no-cache --pull --platform "linux/${arch}" --tag="${build_docker_tag}" "build/deploy-${platform}" 
  GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS docker push "$build_docker_tag" 

#  tc_end_block "Make and push docker images"
fi

gcr_tag="${gcr_repository}:${build_name}"
docker manifest rm "${gcr_tag}" || :
docker manifest create "${gcr_tag}" "${gcr_repository}:${arch}-${build_name}"
docker manifest push "${gcr_tag}"
# tc_end_block "Make and push multi-arch docker images"

# Make finding the tag name easy.
cat << EOF


Build ID: ${build_name}

The binaries will be available at:
  https://storage.googleapis.com/$gcs_bucket/cockroach-$build_name.linux-amd64.tgz
  https://storage.googleapis.com/$gcs_bucket/cockroach-$build_name.linux-amd64-fips.tgz
  https://storage.googleapis.com/$gcs_bucket/cockroach-$build_name.linux-arm64.tgz
  https://storage.googleapis.com/$gcs_bucket/cockroach-$build_name.darwin-10.9-amd64.tgz
  https://storage.googleapis.com/$gcs_bucket/cockroach-$build_name.windows-6.2-amd64.zip

Pull the docker image by:
  docker pull $gcr_repository:$build_name
  docker pull $gcr_repository:$build_name-fips

EOF
