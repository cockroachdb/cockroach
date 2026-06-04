# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

if [ -z "${root:-}" ]
then
    echo '$root is not set; please source teamcity-support.sh'
    exit 1
fi

# FYI: You can run `./dev builder` to run this Docker image. :)
BAZEL_IMAGE=$(cat $root/build/.bazelbuilderversion)

# Capture all "COCKROACH_*" environment variables, generating a string
# in the format:
#
# -e COCKROACH_VAR1 -e COCKROACH_VAR2 ...
#
# This can be passed to the `docker` call in run_bazel, allowing
# engineers to set COCKROACH_* variables via TeamCity and have those
# set in the environment where `cockroach` runs.
#
# NB: `|| true` stops the command from returning 1 if there are no
# COCKROACH_* variables; that would cause builds that use `pipefail`
# to fail.
DOCKER_EXPORT_COCKROACH_VARS=$(env | grep '^COCKROACH_' | cut -d= -f1 | sed -e 's/\(.*\)/-e \1/' | tr '\n' ' ') || true

# Call `run_bazel $NAME_OF_SCRIPT` to start an appropriately-configured Docker
# container with the `us-east1-docker.pkg.dev/crl-ci-images/cockroach/bazel`
# image running the given script.
# BAZEL_SUPPORT_EXTRA_DOCKER_ARGS will be passed on to `docker run` unchanged.
run_bazel() {
    if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
        run_bazel_github "$@"
        return
    fi
    # Set up volumes.
    # TeamCity uses git alternates, so make sure we mount the path to the real
    # git objects.
    teamcity_alternates="/home/agent/system/git"
    vols="--volume ${teamcity_alternates}:${teamcity_alternates}:ro"
    vols="${vols} --volume ${TEAMCITY_BUILD_PROPERTIES_FILE}:${TEAMCITY_BUILD_PROPERTIES_FILE}:ro"
    artifacts_dir=$root/artifacts
    mkdir -p "$artifacts_dir"
    vols="${vols} --volume ${artifacts_dir}:/artifacts"
    cache=/home/agent/.bzlhome24
    mkdir -p $cache
    vols="${vols} --volume ${root}:/go/src/github.com/cockroachdb/cockroach"
    vols="${vols} --volume ${cache}:/home/roach"

    exit_status=0
    docker run -i ${tty-} --rm --init \
        -u "$(id -u):$(id -g)" \
        --workdir="/go/src/github.com/cockroachdb/cockroach" \
	${DOCKER_EXPORT_COCKROACH_VARS} \
	${BAZEL_SUPPORT_EXTRA_DOCKER_ARGS:+$BAZEL_SUPPORT_EXTRA_DOCKER_ARGS} \
        ${vols} \
        $BAZEL_IMAGE "$@" || exit_status=$?
    rm -rf _bazel
    return $exit_status
}

# GitHub Actions sibling of run_bazel. Kept separate so the TC body of
# run_bazel above stays identical to its pre-GHA-migration form, which
# lets TC-side fixes backport cleanly to release branches that don't
# carry the GHA migration.
run_bazel_github() {
    artifacts_dir=$root/artifacts
    mkdir -p "$artifacts_dir"
    vols="--volume ${artifacts_dir}:/artifacts"
    vols="${vols} --volume ${root}:/go/src/github.com/cockroachdb/cockroach"
    cache="${RUNNER_TEMP:-/tmp}/.bzlhome"
    mkdir -p "$cache"
    vols="${vols} --volume ${cache}:/home/roach"
    if [[ -n "${GOOGLE_GHA_CREDS_PATH:-}" && -f "${GOOGLE_GHA_CREDS_PATH}" ]]; then
        vols="${vols} --volume ${GOOGLE_GHA_CREDS_PATH}:${GOOGLE_GHA_CREDS_PATH}:ro"
        BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="${BAZEL_SUPPORT_EXTRA_DOCKER_ARGS:-} -e CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=${GOOGLE_GHA_CREDS_PATH} -e GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_GHA_CREDS_PATH}"
    fi

    exit_status=0
    docker run -i ${tty-} --rm --init \
        -u "$(id -u):$(id -g)" \
        --workdir="/go/src/github.com/cockroachdb/cockroach" \
	${DOCKER_EXPORT_COCKROACH_VARS} \
	${BAZEL_SUPPORT_EXTRA_DOCKER_ARGS:+$BAZEL_SUPPORT_EXTRA_DOCKER_ARGS} \
        ${vols} \
        $BAZEL_IMAGE "$@" || exit_status=$?
    rm -rf _bazel
    return $exit_status
}

# configure_bazel_storage_access_token mints a short-lived GCS access token
# from a service-account key and exports it as $BAZEL_STORAGE_ACCESS_TOKEN.
# build/bazelutil/credential-helper (wired into .bazelrc) reads this token to
# authenticate Bazel's downloads of Go module dependencies from the private
# cockroach-godeps GCS bucket.
#
# The key is taken from $1 if given, otherwise from $GOOGLE_EPHEMERAL_CREDENTIALS.
# Nightly builds have the ephemeral key in their environment and call this with
# no arguments; release/customized builds run under a different service account
# and pass that account's key explicitly. Whichever account is used must hold
# roles/storage.objectViewer on gs://cockroach-godeps-private.
#
# This is opt-in: only the configurations that build or test via Bazel call it,
# so the rest of CI keeps using run_bazel unchanged. Callers that build inside
# the builder container (run_bazel) must also forward the variable into the
# container by adding `-e BAZEL_STORAGE_ACCESS_TOKEN` to
# $BAZEL_SUPPORT_EXTRA_DOCKER_ARGS. Callers that invoke Bazel directly on the
# agent need only call this function; the bazel process inherits the exported
# variable.
#
# Minting on the agent (rather than inside the container) keeps the long-lived
# key out of the container: only the ~1h token crosses the boundary. That TTL
# comfortably covers the dependency-fetch phase of a build; the credential
# helper is not consulted again once the external repositories have been
# materialized.
#
# The token is minted via gcloud where it is available. Some agents have no
# gcloud build (Google ships no Cloud SDK for s390x), so we fall back to minting
# the token directly from the key via the OAuth 2.0 JWT-bearer flow, which needs
# only openssl, curl, and python3.
configure_bazel_storage_access_token() {
  local creds="${1:-${GOOGLE_EPHEMERAL_CREDENTIALS:-}}"
  : "${creds:?a service-account key must be provided to authenticate to the private cockroach-godeps bucket}"
  # Disable xtrace while the key and token are in flight; some callers run with
  # `set -x`, which would otherwise echo the credential to the build log.
  local xtrace_was_on=0
  case "$-" in *x*) xtrace_was_on=1; set +x;; esac
  local keyfile token rc=0
  keyfile=$(mktemp)
  printf '%s' "${creds}" > "${keyfile}"
  # Capture the mint result via `|| rc=$?` rather than a bare assignment so a
  # failure does not abort here under `set -e`: we want to clean up the key and
  # surface a clear error regardless of which path ran.
  if command -v gcloud >/dev/null 2>&1; then
    token=$(_mint_storage_access_token_gcloud "${keyfile}") || rc=$?
  else
    token=$(_mint_storage_access_token_jwt "${keyfile}") || rc=$?
  fi
  rm -f "${keyfile}"
  [[ "${xtrace_was_on}" == 1 ]] && set -x
  if [[ "${rc}" -ne 0 || -z "${token}" ]]; then
    echo "failed to mint \$BAZEL_STORAGE_ACCESS_TOKEN; Bazel will be unable to fetch private dependencies" >&2
    return 1
  fi
  export BAZEL_STORAGE_ACCESS_TOKEN="${token}"
  return 0
}

# _mint_storage_access_token_gcloud prints an access token for the key in the
# file $1, using the gcloud CLI. The service account is activated under an
# isolated CLOUDSDK_CONFIG so the agent's default gcloud account, which other
# build steps rely on, is left untouched.
_mint_storage_access_token_gcloud() {
  local keyfile="$1"
  local sdkconfig token rc=0
  sdkconfig=$(mktemp -d)
  # Capture into `token` rather than printing print-access-token directly, so the
  # config directory is always removed below and a gcloud failure still
  # propagates as this function's exit status (a trailing `rm` would mask it, and
  # an EXIT trap set here would leak into the caller's shell).
  CLOUDSDK_CONFIG="${sdkconfig}" gcloud auth activate-service-account \
    --key-file="${keyfile}" >/dev/null 2>&1 || rc=$?
  if [[ "${rc}" -eq 0 ]]; then
    token=$(CLOUDSDK_CONFIG="${sdkconfig}" gcloud auth print-access-token) || rc=$?
  fi
  rm -rf "${sdkconfig}"
  [[ "${rc}" -eq 0 ]] || return "${rc}"
  printf '%s' "${token}"
}

# _mint_storage_access_token_jwt prints an access token for the key in the file
# $1 without gcloud, implementing the OAuth 2.0 JWT-bearer flow (RFC 7523): it
# builds a JWT asserting the service account's identity, signs it with the
# account's RSA private key (RS256, via openssl), and exchanges it for a token
# at the account's token endpoint. The scope is read-only storage, which is all
# the dependency fetch needs. python3 parses the key because the PEM private key
# carries embedded newlines that text tools mangle.
_mint_storage_access_token_jwt() {
  local keyfile="$1"
  local client_email token_uri now exp
  client_email=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["client_email"])' "${keyfile}")
  token_uri=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("token_uri", "https://oauth2.googleapis.com/token"))' "${keyfile}")
  now=$(date +%s)
  exp=$((now + 3600))

  local header_b64 claims_b64 signing_input sig_b64 assertion pemfile
  header_b64=$(printf '%s' '{"alg":"RS256","typ":"JWT"}' | _base64url)
  claims_b64=$(printf '{"iss":"%s","scope":"https://www.googleapis.com/auth/devstorage.read_only","aud":"%s","iat":%s,"exp":%s}' \
    "${client_email}" "${token_uri}" "${now}" "${exp}" | _base64url)
  signing_input="${header_b64}.${claims_b64}"
  pemfile=$(mktemp)
  python3 -c 'import json,sys; sys.stdout.write(json.load(open(sys.argv[1]))["private_key"])' "${keyfile}" > "${pemfile}"
  sig_b64=$(printf '%s' "${signing_input}" | openssl dgst -sha256 -sign "${pemfile}" -binary | _base64url)
  # Remove the key as soon as signing is done, to minimize the window it is on
  # disk; this also runs on the error paths below, since errexit is not in effect
  # inside this command-substitution subshell.
  rm -f "${pemfile}"
  assertion="${signing_input}.${sig_b64}"

  local response
  response=$(curl -sS -X POST "${token_uri}" \
    --data-urlencode 'grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer' \
    --data-urlencode "assertion=${assertion}")
  # On success the response carries the access token, so only echo it on the
  # failure path, where it instead holds Google's (non-secret) error body, e.g.
  # {"error":"invalid_grant","error_description":"..."} — invaluable in CI logs.
  printf '%s' "${response}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])' 2>/dev/null || {
      echo "failed to mint storage access token; token endpoint response: ${response}" >&2
      return 1
    }
}

# _base64url base64url-encodes stdin with padding stripped (per JWT). openssl's
# -A keeps the output on a single line; the surrounding $() strips the trailing
# newline.
_base64url() {
  openssl base64 -A | tr '+/' '-_' | tr -d '='
}

# local copy of _tc_build_branch from teamcity-support.sh to avoid imports.
_tc_build_branch() {
    echo "${TC_BUILD_BRANCH#refs/heads/}"
}

# local copy of tc_release_branch from teamcity-support.sh to avoid imports.
_tc_release_branch() {
  branch=$(_tc_build_branch)
  [[ "$branch" == master || "$branch" == release-* || "$branch" == provisional_* ]]
}
