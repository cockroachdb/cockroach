#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -xeuo pipefail

# This script expects the following env vars to be set:
#   BRANCH [branch name to be created in fork]
#   VERSION [new version to echo into build/.bazelbuilderversion]
#   GH_TOKEN [github token]

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/teamcity-common-support.sh"
ssh_key_dir="$(pwd)"

git_ssh() {
  # $@ passes all arguments to this function to the command
  GIT_SSH_COMMAND="ssh -i $ssh_key_dir/.cockroach-teamcity-key" git "$@"
}

# Install `gh` tool.
wget -O /tmp/gh.tar.gz https://github.com/cli/cli/releases/download/v2.13.0/gh_2.13.0_linux_amd64.tar.gz
echo "9e833e02428cd49e0af73bc7dc4cafa329fe3ecba1bfe92f0859bf5b11916401  /tmp/gh.tar.gz" | sha256sum -c -
tar --strip-components 1 -xf /tmp/gh.tar.gz
export PATH=$PWD/bin:$PATH

# Configure git.
git config --global user.email "teamcity@cockroachlabs.com"
git config --global user.name "cockroach-teamcity"
configure_git_ssh_key
trap "rm -f $ssh_key_dir/.cockroach-teamcity-key" EXIT
WORKDIR="$(mktemp -d ./workdir.XXXXXX)"

git_ssh clone "ssh://git@github.com/cockroachdb/cockroach.git" "$WORKDIR/cockroach" && cd "$WORKDIR/cockroach"

# Push commit to fork.
git checkout -b "$BRANCH"
if [[ $VERSION == *"-fips:"* ]]; then
  IMG=bazel-fips
  echo -n "$VERSION" > build/.bazelbuilderversion-fips
else
  IMG=bazel
  echo -n "$VERSION" > build/.bazelbuilderversion
fi
git commit -a -m "ci: update $IMG builder image

Release note: None
Epic: None"
git_ssh push "ssh://git@github.com/cockroach-teamcity/cockroach.git" $BRANCH

# Create PR.
gh pr create --fill --head="cockroach-teamcity:$BRANCH" --base="master"

rm -rf $WORKDIR
