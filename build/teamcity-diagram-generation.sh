#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"
source "$(dirname "${0}")/teamcity-bazel-support.sh"

cockroach_ref=$(git describe --tags --exact-match 2>/dev/null || git rev-parse HEAD)

git clone https://github.com/cockroachdb/generated-diagrams.git

# Update diagrams on Github.
export GIT_AUTHOR_NAME="Cockroach TeamCity"
export GIT_COMMITTER_NAME="Cockroach TeamCity"
export GIT_AUTHOR_EMAIL="teamcity@cockroachlabs.com"
export GIT_COMMITTER_EMAIL="teamcity@cockroachlabs.com"

cd generated-diagrams

git checkout $TC_BUILD_BRANCH || git checkout -b $TC_BUILD_BRANCH

# Clean out old diagrams.
rm -rf bnf && mkdir bnf
rm -rf grammar_svg && mkdir grammar_svg

tc_start_block "Generate Diagrams"
# Must run this from the root.
cd ..
run_bazel build/teamcity/cockroach/publish-sql-grammar-diagrams-impl.sh
cp $root/artifacts/bazel-bin/docs/generated/sql/bnf/*.bnf ./generated-diagrams/bnf
cp $root/artifacts/bazel-bin/docs/generated/sql/bnf/*.html ./generated-diagrams/grammar_svg
tc_end_block "Generate Diagrams"

tc_start_block "Push Diagrams to Git"
cd generated-diagrams

changed_diagrams=$(git status --porcelain)
# Check if the branch exists remotely
branch_exists=$(git ls-remote --heads ssh://git@github.com/cockroachdb/generated-diagrams.git "$TC_BUILD_BRANCH")
if [ -z "$changed_diagrams" ]; then
    if [ "$TC_BUILD_BRANCH" = "master" ]; then
        echo "No diagrams changed on master branch. Exiting."
        tc_end_block "Push Diagrams to Git"
        exit 0
    elif [ -z "$branch_exists" ]; then
        echo "Branch $TC_BUILD_BRANCH does not exist. Creating empty commit."
        echo "No diagrams changed, but creating branch $TC_BUILD_BRANCH anyway."
        git commit --allow-empty -m "Empty commit to create branch $TC_BUILD_BRANCH for reference $cockroach_ref.This empty commit is required to ensure the branch exists in the remote repository even when there are no diagram changes. This helps maintain branch consistency with cockroach repo to ensure clean release note generation process "
    else
        echo "Branch $TC_BUILD_BRANCH already exists. Skipping empty commit."
        tc_end_block "Push Diagrams to Git"
        exit 0
    fi
else
    git add .
    git commit -m "Snapshot $cockroach_ref"
fi

github_ssh_key="${PRIVATE_DEPLOY_KEY_FOR_GENERATED_DIAGRAMS}"
configure_git_ssh_key

git_wrapped push -f ssh://git@github.com/cockroachdb/generated-diagrams.git
tc_end_block "Push Diagrams to Git"
