#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
root="$(cd "${script_dir}/../../../.." &> /dev/null && pwd)"

source "$root/build/teamcity-bazel-support.sh" # For BAZEL_IMAGE.

PROJECT="${PROJECT:-cockroach-testeng-infra}"
REGION="${REGION:-us-east1}"
DLQ_ENV="${DLQ_ENV:-prod}"
case "$DLQ_ENV" in
  prod | dev)
    ;;
  *)
    echo "DLQ_ENV must be prod or dev" >&2
    exit 1
    ;;
esac
DEFAULT_RESOURCE="roachtest-github-dlq-replay-${DLQ_ENV}"
JOB="${JOB:-${DEFAULT_RESOURCE}}"
REPOSITORY="${REPOSITORY:-${DEFAULT_RESOURCE}}"
IMAGE_NAME="${IMAGE_NAME:-replay}"
IMAGE="${IMAGE:-${REGION}-docker.pkg.dev/${PROJECT}/${REPOSITORY}/${IMAGE_NAME}}"
REMOTE="${REMOTE:-origin}"

parse_github_remote() {
  local url="$1"
  local path
  case "$url" in
    git@github.com:*)
      path="${url#git@github.com:}"
      ;;
    https://github.com/*)
      path="${url#https://github.com/}"
      ;;
    ssh://git@github.com/*)
      path="${url#ssh://git@github.com/}"
      ;;
    *)
      return 1
      ;;
  esac
  path="${path%.git}"
  printf '%s\n' "$path"
}

remote_url="$(git -C "$root" remote get-url "$REMOTE")"
remote_path="$(parse_github_remote "$remote_url" || true)"
if [[ -n "$remote_path" && "$remote_path" == */* ]]; then
  OWNER="${OWNER:-${remote_path%%/*}}"
  REPO="${REPO:-${remote_path#*/}}"
fi
OWNER="${OWNER:-cockroachdb}"
REPO="${REPO:-cockroach}"

GIT_SHA="$(git -C "$root" rev-parse HEAD)"
IMAGE_TAG="${IMAGE_TAG:-$(git -C "$root" rev-parse --short HEAD)}"

if [[ "${ALLOW_DIRTY:-0}" != "1" ]]; then
  if ! git -C "$root" diff --quiet || ! git -C "$root" diff --cached --quiet; then
    cat >&2 <<'EOF'
Working tree has uncommitted tracked changes.
Commit them before pushing the Cloud Run image, or set ALLOW_DIRTY=1 if you
intentionally want the image to be built from the current committed SHA only.
EOF
    exit 1
  fi
fi

cat <<EOF
Building and pushing dlq-replay image:
  source:  https://github.com/${OWNER}/${REPO}@${GIT_SHA}
  image:   ${IMAGE}:${IMAGE_TAG}
  project: ${PROJECT}
  env:     ${DLQ_ENV}
  job:     ${JOB}
  region:  ${REGION}
EOF

(
  cd "$script_dir"
  gcloud --project "$PROJECT" builds submit \
    --substitutions="_BAZEL_IMAGE=${BAZEL_IMAGE},_GIT_SHA=${GIT_SHA},_IMAGE=${IMAGE},_IMAGE_TAG=${IMAGE_TAG},_OWNER=${OWNER},_REPO=${REPO}" \
    --timeout="${CLOUD_BUILD_TIMEOUT:-30m}"
)

gcloud --project "$PROJECT" run jobs update "$JOB" \
  --region "$REGION" \
  --image "${IMAGE}:${IMAGE_TAG}"

echo "Updated Cloud Run Job ${JOB} to ${IMAGE}:${IMAGE_TAG}"
