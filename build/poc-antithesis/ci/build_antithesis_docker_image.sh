#!/usr/bin/env bash
set -euo pipefail

dir="$(dirname $(dirname $(dirname "${0}")))"
source "$dir/teamcity-support.sh"  # For $root
source "$dir/release/teamcity-support.sh" # For configure_docker_creds, docker_login_with_google

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"

export ANTITHESIS_ARTIFACTS=$PWD/artifacts/instrument
mkdir -p "$ANTITHESIS_ARTIFACTS"

tc_start_block "Variable Setup"
build_name=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)

if [ -z "${BUILD_TAG}" ]; then
  build_tag=$(git rev-parse --abbrev-ref HEAD)
else
  # user-specified BUILD_TAG overrides current branch name
  build_tag=${BUILD_TAG}
fi

# On no match, `grep -Eo` returns 1. `|| echo""` makes the script not error.
release_branch="$(echo "$build_name" | grep -Eo "^v[0-9]+\.[0-9]+" || echo"")"

# Used for docker login for gcloud
google_credentials="$GOOGLE_TESTENG_INFRA_CREDENTIALS"
# Used for docker push to Antithesis repo
antithesis_credentials="$ANTITHESIS_REPO_CREDENTIALS"
gcr_repository="gcr.io/cockroach-testeng-infra"
gcr_hostname="gcr.io"
antithesis_repository="us-central1-docker.pkg.dev/molten-verve-216720/cockroachdb-repository"
antithesis_hostname="us-central1-docker.pkg.dev"

cat << EOF

  build_name:      $build_name
  build_tag:       $build_tag
  release_branch:  $release_branch
  gcr_repository:  $gcr_repository

EOF

tc_end_block "Variable Setup"

tc_start_block "Docker configure"
configure_docker_creds
docker_login_with_google
# need to add $antithesis_hostname to docker auth.
echo "${antithesis_credentials}" | docker login -u _json_key --password-stdin "https://${antithesis_hostname}"

# need to pull our custom builder from private GCR
docker pull $gcr_repository/cockroachdb/builder:antithesis-latest

tc_end_block "Docker configure"

# Subsequent steps are run relative to poc-antithesis
cd $dir/poc-antithesis

tc_start_block "Compile instrumented crdb (short) binary"

# N.B. antithesis' goinstrumenter uses 'cp -R' which requires that the instrumentation directory is not a child
# N.B. instrumented binary, symbols and shared lib are copied into $ANTITHESIS_ARTIFACTS (see Makefile)
run ./builder.sh mkrelease amd64-linux-gnu instrumentshort INSTRUMENTATION_TMP=/go/src/github.com/instrument
tc_end_block "Compile instrumented crdb (short) binary"

tc_start_block "Copy instrumented binary and dependency files to build/deploy"
run ./prepare-deploy.sh

tc_end_block "Copy instrumented binary and dependency files to build/deploy"

tc_start_block "Build cockroach-instrumented docker image"

docker build \
  --no-cache \
  --tag="cockroachdb/cockroach-instrumented:$build_tag" \
  --memory 30g \
  --memory-swap -1 \
  -f deploy/Dockerfile_antithesis_instrumented \
  deploy
tc_end_block "Build cockroach-instrumented docker image"

tc_start_block "Build workload docker image"

docker build \
  --no-cache \
  --tag="cockroachdb/workload:$build_tag" \
  --memory 30g \
  --memory-swap -1 \
  -f deploy/Dockerfile_antithesis_workload \
  deploy
tc_end_block "Build workload docker image"

tc_start_block "Build docker-compose config. image"

docker build \
  --no-cache \
  --tag="cockroachdb/config:$build_tag" \
  --memory 30g \
  --memory-swap -1 \
  -f deploy/Dockerfile_antithesis_config \
  deploy
tc_end_block "Build docker-compose config. image"

tc_start_block "Pushing docker images to GCR"

docker tag cockroachdb/cockroach-instrumented:$build_tag $gcr_repository/cockroachdb/cockroach-instrumented:$build_tag
docker tag cockroachdb/workload:$build_tag  $gcr_repository/cockroachdb/workload:$build_tag
docker tag cockroachdb/config:$build_tag  $gcr_repository/cockroachdb/config:$build_tag

docker push $gcr_repository/cockroachdb/cockroach-instrumented:$build_tag
docker push $gcr_repository/cockroachdb/workload:$build_tag
docker push $gcr_repository/cockroachdb/config:$build_tag
tc_end_block "Pushing docker images to GCR"

tc_start_block "Verifying docker-compose"

export docker_registry=$gcr_repository
export build_tag=$build_tag
export data_dir=$artifacts/data
# verify docker-compose; workload takes 5 minutes, plus init. time; so, give it a slack of another 5 mins.
run timeout 600 deploy/verify_docker_compose.sh
tc_end_block "Verifying docker-compose"

tc_start_block "Pushing docker images to Antithesis"

docker tag cockroachdb/cockroach-instrumented:$build_tag $antithesis_repository/cockroachdb/cockroach-instrumented:$build_tag
docker tag cockroachdb/workload:$build_tag  $antithesis_repository/cockroachdb/workload:$build_tag
docker tag cockroachdb/config:$build_tag  $antithesis_repository/cockroachdb/config:$build_tag

docker push $antithesis_repository/cockroachdb/cockroach-instrumented:$build_tag
docker push $antithesis_repository/cockroachdb/workload:$build_tag
docker push $antithesis_repository/cockroachdb/config:$build_tag
tc_end_block "Pushing docker images to Antithesis"
