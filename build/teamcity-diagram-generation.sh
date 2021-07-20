#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_start_block "Get Railroad Jar"
log_into_gcloud
gsutil cp gs://cockroach-railroad-jar/Railroad-1.45-java7/Railroad.jar ./Railroad.jar
railroadPath=`pwd`/Railroad.jar
tc_end_block "Get Railroad Jar"

cockroach_ref=$(git describe --tags --exact-match 2>/dev/null || git rev-parse HEAD)

make bin/docgen

git clone https://github.com/cockroachdb/generated-diagrams.git

# Update diagrams on Github.
export GIT_AUTHOR_NAME="Cockroach TeamCity"
export GIT_COMMITTER_NAME="Cockroach TeamCity"
export GIT_AUTHOR_EMAIL="teamcity@cockroachlabs.com"
export GIT_COMMITTER_EMAIL="teamcity@cockroachlabs.com"

cd generated-diagrams

git checkout $TC_BUILD_BRANCH || git checkout -b $TC_BUILD_BRANCH

# Clean out old diagrams.
rm -rf bnf
rm -rf grammar_svg

tc_start_block "Generate Diagrams"
# docgen has to be run in the cockroach root directory to find
# the sql.y grammar file.
cd ..
bin/docgen grammar bnf ./generated-diagrams/bnf
bin/docgen grammar svg ./generated-diagrams/bnf ./generated-diagrams/grammar_svg --railroad $railroadPath
tc_end_block "Generate Diagrams"

tc_start_block "Push Diagrams to Git"
cd generated-diagrams

git add .
git commit -m "Snapshot $cockroach_ref"

github_ssh_key="${PRIVATE_DEPLOY_KEY_FOR_GENERATED_DIAGRAMS}"
configure_git_ssh_key

git_wrapped push -f ssh://git@github.com/cockroachdb/generated-diagrams.git
tc_end_block "Push Diagrams to Git"
