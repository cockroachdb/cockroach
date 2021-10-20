#!/usr/bin/env bash

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

git add .
git commit -m "Snapshot $cockroach_ref"

github_ssh_key="${PRIVATE_DEPLOY_KEY_FOR_GENERATED_DIAGRAMS}"
configure_git_ssh_key

git_wrapped push -f ssh://git@github.com/cockroachdb/generated-diagrams.git
tc_end_block "Push Diagrams to Git"
