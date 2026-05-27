#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

to=dev-inf+release-dev@cockroachlabs.com
dry_run=true
version_bump_only=false
# Production destination (real email, dry_run=false) requires both DRY_RUN
# unset AND running on the production release repo. IS_PRODUCTION_REPO is
# forwarded from a GHA repository variable (set only on the canonical repo)
# and is absent under TeamCity; default to "true" so the TC code path keeps
# its existing behavior.
if [[ -z "${DRY_RUN}" && "${IS_PRODUCTION_REPO:-true}" == "true" ]] ; then
  echo "Setting production values"
  to=release-engineering-team@cockroachlabs.com
  dry_run=false
fi

if [[ -n "${VERSION_BUMP_ONLY:-}" ]] ; then
  version_bump_only=true
fi

# Configure git to authenticate to github.com using GH_TOKEN. Inside the
# bazel container the auth that actions/checkout writes to the host's
# .git/config is not reliably picked up, so subsequent `git fetch` /
# `git ls-remote` invocations would otherwise prompt for a username and
# abort with:
#   fatal: could not read Username for 'https://github.com'
#
# Embed the token in the URL via insteadOf rewrite (rather than an
# extraheader, which GitHub's git-over-HTTPS server rejects in the
# Bearer form). The rewrite only matches https://github.com/* URLs, so
# under TeamCity (where origin is typically SSH) it is a no-op and the
# existing TC SSH auth path is preserved.
#
# Use GIT_CONFIG_COUNT/_KEY/_VALUE rather than `git config --local` so the
# credentialed URL exists only in this script's process environment — never
# on disk in .git/config, which lives on the bind-mounted workspace and
# would survive the bazel container's --rm. Suppress xtrace around the
# assignment so `set -x` doesn't echo the token to stderr (where masking
# may not catch a token embedded inside a URL substring).
if [[ -n "${GH_TOKEN:-}" ]]; then
  { set +x; } 2>/dev/null
  export GIT_CONFIG_COUNT=1
  export GIT_CONFIG_KEY_0="url.https://x-access-token:${GH_TOKEN}@github.com/.insteadOf"
  export GIT_CONFIG_VALUE_0="https://github.com/"
  set -x
fi

# run git fetch in order to get all remote branches
git fetch --tags -q origin

# install gh and helm
curl -fsSL -o /tmp/gh.tar.gz https://github.com/cli/cli/releases/download/v2.32.1/gh_2.32.1_linux_amd64.tar.gz
echo "5c9a70b6411cc9774f5f4e68f9227d5d55ca0bfbd00dfc6353081c9b705c8939  /tmp/gh.tar.gz" | sha256sum -c -
tar --strip-components 1 -xf /tmp/gh.tar.gz
curl -fsSL -o /tmp/helm.tar.gz https://get.helm.sh/helm-v3.14.1-linux-amd64.tar.gz
echo "75496ea824f92305ff7d28af37f4af57536bf5138399c824dff997b9d239dd42  /tmp/helm.tar.gz" | sha256sum -c -
tar -C bin --strip-components 1 -xf /tmp/helm.tar.gz linux-amd64/helm
export PATH=$PWD/bin:$PATH

bazel build --config=crosslinux //pkg/cmd/release

# --cockroach-repo picks the push target. Resolution, in order:
#   1. $COCKROACH_REPO if the operator explicitly set it.
#   2. $GITHUB_REPOSITORY if we're under GHA (auto-set by the runner;
#      becomes "owner/name" of whatever repo dispatched the workflow).
#   3. cockroachdb/cockroach if running under TeamCity ($TEAMCITY_VERSION
#      set) — historical TC behavior preserved as a literal here, in
#      the script, rather than baked into the Go binary.
# Otherwise the binary errors out: --cockroach-repo is required and
# local/foreign invocations must supply it explicitly.
if [[ -n "${COCKROACH_REPO:-}" ]]; then
  cockroach_repo="$COCKROACH_REPO"
elif [[ -n "${GITHUB_REPOSITORY:-}" ]]; then
  cockroach_repo="$GITHUB_REPOSITORY"
elif [[ -n "${TEAMCITY_VERSION:-}" ]]; then
  cockroach_repo="cockroachdb/cockroach"
else
  echo "ERROR: cannot derive --cockroach-repo. Set COCKROACH_REPO env var, run under GHA, or run under TeamCity." >&2
  exit 1
fi

# Force IS_PRODUCTION_REPO=true under TeamCity (default-on for the
# historical TC-as-prod assumption). The Go binary's isProductionRepo()
# is the dry-run override trigger in generateRepoList, replacing the
# previous flag-equals-literal check; under TC we want that trigger to
# fire so dry-runs continue to push to crltest/* forks. GHA workflows
# set IS_PRODUCTION_REPO explicitly via vars.IS_PRODUCTION_REPO; local
# invocations get false unless the operator sets it.
if [[ -n "${TEAMCITY_VERSION:-}" ]]; then
  export IS_PRODUCTION_REPO="${IS_PRODUCTION_REPO:-true}"
fi
# --github-username controls which user pushes the PR branches and opens
# the PRs. TC keeps the historical "cockroach-teamcity" default; the GHA
# wrapper sets GITHUB_USERNAME to the bot account that owns its PAT
# (e.g. "crl-release-eng-bot"). prExists() in the binary searches across
# both authors regardless, so prior PRs aren't re-opened during the
# migration period.
github_username="${GITHUB_USERNAME:-cockroach-teamcity}"

$(bazel info --config=crosslinux bazel-bin)/pkg/cmd/release/release_/release \
  update-versions \
  --dry-run=$dry_run \
  --version-bump-only=$version_bump_only \
  --released-version=$RELEASED_VERSION \
  --next-version=$NEXT_VERSION \
  --template-dir=pkg/cmd/release/templates \
  --smtp-user=cronjob@cockroachlabs.com \
  --smtp-host=smtp.gmail.com \
  --smtp-port=587 \
  --artifacts-dir=/artifacts \
  --cockroach-repo="$cockroach_repo" \
  --github-username="$github_username" \
  --to=$to
