#!/usr/bin/env bash

set -xeuo pipefail

to=dev-inf+release-dev@cockroachlabs.com
dry_run=true
# override dev defaults with production values
if [[ -z "${DRY_RUN}" ]] ; then
  echo "Setting production values"
  to=release-engineering-team@cockroachlabs.com
  dry_run=false
fi

# run git fetch in order to get all remote branches
git remote -v
git tag --list
git fetch --tags -q origin
git tag --list

# install gh
wget -O /tmp/gh.tar.gz https://github.com/cli/cli/releases/download/v2.29.0/gh_2.29.0_linux_amd64.tar.gz
echo "9fe05f43a11a7bf8eacf731422452d1997e6708d4160ef0efcb13c103320390e  /tmp/gh.tar.gz" | sha256sum -c -
tar --strip-components 1 -xf /tmp/gh.tar.gz
export PATH=$PWD/bin:$PATH

bazel build --config=crosslinux //pkg/cmd/release

$(bazel info --config=crosslinux bazel-bin)/pkg/cmd/release/release_/release \
  update-versions \
  --dry-run=$dry_run \
  --released-version=$RELEASED_VERSION \
  --next-version=$NEXT_VERSION \
  --template-dir=pkg/cmd/release/templates \
  --smtp-user=cronjob@cockroachlabs.com \
  --smtp-host=smtp.gmail.com \
  --smtp-port=587 \
  --to=$to
