#!/usr/bin/env bash
set -euo pipefail

# BRANCH=$1
# VERSION=$2
REMOTE="cockroach-teamcity"
COMMIT_MESSAGE="ci: update bazel builder image
Release note: None
Epic: None"

git checkout -b "$BRANCH"
echo "$VERSION" > build/.bazelbuilderversion
git commit -a -m "$COMMIT_MESSAGE"
git push "$REMOTE" "$BRANCH"

# install gh
wget -O /tmp/gh.tar.gz https://github.com/cli/cli/releases/download/v2.13.0/gh_2.13.0_linux_amd64.tar.gz
echo "9e833e02428cd49e0af73bc7dc4cafa329fe3ecba1bfe92f0859bf5b11916401  /tmp/gh.tar.gz" | sha256sum -c -
tar --strip-components 1 -xf /tmp/gh.tar.gz
export PATH=$PWD/bin:$PATH

gh pr create --fill --head="$REMOTE/$BRANCH" --base="origin/master"
