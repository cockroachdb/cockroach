# Copyright 2020 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Common helpers for teamcity-*.sh scripts.

# root is the absolute path to the root directory of the repository.
root="$(dirname $(dirname $(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )))"
source "$root/build/teamcity-common-support.sh"

remove_files_on_exit() {
  rm -rf ~/.docker
  common_support_remove_files_on_exit
}
trap remove_files_on_exit EXIT

tc_start_block() {
  echo "##teamcity[blockOpened name='$1']"
}

tc_end_block() {
  echo "##teamcity[blockClosed name='$1']"
}

docker_login_with_google() {
  # https://cloud.google.com/container-registry/docs/advanced-authentication#json-key
  echo "${google_credentials}" | docker login -u _json_key --password-stdin "https://${gcr_hostname}"
}

docker_login_gcr() {
  local repo=$1
  local credentials=$2
  local hostname="${repo%%/*}"
  # https://cloud.google.com/container-registry/docs/advanced-authentication#json-key
  echo "${credentials}" | docker login -u _json_key --password-stdin "https://${hostname}"
}

docker_login() {
  echo "${DOCKER_ACCESS_TOKEN}" | docker login --username "${DOCKER_ID}" --password-stdin
}

configure_docker_creds() {
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
}

docker_login_with_redhat() {
  echo "${REDHAT_REGISTRY_KEY}" | docker login --username unused --password-stdin $rhel_registry
}

verify_docker_image(){
  local img=$1
  local docker_platform=$2
  local expected_sha=$3
  local expected_build_tag=$4
  local fips_build=$5
  local error=0

  docker rmi "$img" || true
  docker pull --platform="$docker_platform" "$img"

  local output=$(docker run --platform="$docker_platform" "$img" version)
  build_type=$(grep "^Build Type:" <<< "$output" | cut -d: -f2 | sed 's/ //g')
  sha=$(grep "^Build Commit ID:" <<< "$output" | cut -d: -f2 | sed 's/ //g')
  build_tag=$(grep "^Build Tag:" <<< "$output" | cut -d: -f2 | sed 's/ //g')
  go_version=$(grep "^Go Version:" <<< "$output" | cut -d: -f2 | sed 's/ //g')

  # Build Type should always be "release"
  if [ "$build_type" != "release" ]; then
    echo "ERROR: Release type mismatch, expected 'release', got '$build_type'"
    error=1
  fi
  if [ "$sha" != "$expected_sha" ]; then
    echo "ERROR: SHA mismatch, expected '$expected_sha', got '$sha'"
    error=1
  fi
  if [ "$build_tag" != "$expected_build_tag" ]; then
    echo "ERROR: Build tag mismatch, expected '$expected_build_tag', got '$build_tag'"
    error=1
  fi

  build_tag_output=$(docker run --platform="$docker_platform" "$img" version --build-tag)
  if [ "$build_tag_output" != "$expected_build_tag" ]; then
    echo "ERROR: Build tag from 'cockroach version --build-tag' mismatch, expected '$expected_build_tag', got '$build_tag_output'"
    error=1
  fi
  if [[ $fips_build == true ]]; then
    if [[ "$go_version" != *"fips"* ]]; then
      echo "ERROR: Go version '$go_version' does not contain 'fips'"
      error=1
    fi
    openssl_version_output=$(docker run --platform="$docker_platform" "$img" shell -c "openssl version -f")
    if [[ $openssl_version_output != *"FIPS_VERSION"* ]]; then
      echo "ERROR: openssl version '$openssl_version_output' does not contain 'FIPS_VERSION'"
      error=1
    fi
  fi
  return $error
}


function is_release_or_master_build(){
  # On no match, `grep -Eo` returns 1. `|| echo""` makes the script not error.
  echo "$1" | grep -Eo "^((staging|release|rc)-(v)?[0-9][0-9]\.[0-9]).*|master$" || echo ""
  #                        ^ Match branches that start with "staging" (extra-ordinary relases),
  #                        "release" (regular releases, including baking releases, e.g. release-v23.1.15-rc),
  #                        "rc" (will be used for baking releases in the furure).
  #                                             ^ "v" is optional to match main release branches, e.g. release-23.2
  #                                                ^ calver prefix, e.g. 25.1
  # We don't strictly match the suffix to allow different ones, e.g. "rc" or have none.
}

# Compare the passed version to the latest published version. Returns 0 if the
# passed version is the latest. Supports stable versions only.
function is_latest() {
  version=$1
  url="https://get.cockroachdb.com/api/is_latest?version=$version"
  maybe_latest="$(curl -fsSL "$url" || echo "")"
  if [[ $maybe_latest == "yes" ]]; then
    return 0
  fi
  return 1
}
