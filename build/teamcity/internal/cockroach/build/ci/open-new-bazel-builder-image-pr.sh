#!/usr/bin/env bash
set -xeuo pipefail

# This script expects the following env vars to be set:
#   BRANCH [branch name to be created in fork]
#   VERSION [new version to echo into build/.bazelbuilderversion]
#   GH_TOKEN [github token]

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/teamcity-common-support.sh"

# Configure git.
git config --global user.email "teamcity@cockroachlabs.com"
git config --global user.name "cockroach-teamcity"
git remote set-url origin "ssh://git@github.com/cockroachdb/cockroach.git"
configure_git_ssh_key
git_wrapped fetch -q origin

# Push commit to fork.
git checkout -b "$BRANCH"
echo -n "$VERSION" > build/.bazelbuilderversion
git commit -a -m "ci: update bazel builder image
Release note: None
Epic: None"
git_wrapped push "ssh://git@github.com/cockroach-teamcity/cockroach.git" $BRANCH

# Install `gh` tool.
wget -O /tmp/gh.tar.gz https://github.com/cli/cli/releases/download/v2.13.0/gh_2.13.0_linux_amd64.tar.gz
echo "9e833e02428cd49e0af73bc7dc4cafa329fe3ecba1bfe92f0859bf5b11916401  /tmp/gh.tar.gz" | sha256sum -c -
tar --strip-components 1 -xf /tmp/gh.tar.gz
export PATH=$PWD/bin:$PATH

# Create PR.
gh pr create --fill --head="cockroach-teamcity:$BRANCH" --base="master"
