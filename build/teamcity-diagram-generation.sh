#!/usr/bin/env bash

source "$(dirname "${0}")/teamcity-common-support.sh"

# Grab the Railroad jar for SVG diagram generation.
log_into_gcloud
gsutil cp gs://cockroach-railroad-jar/Railroad-1.45-java7/Railroad.jar ./Railroad.jar

railroadPath=`pwd`/Railroad.jar

ref=$(git describe --tags --exact-match 2>/dev/null || git rev-parse HEAD)

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

# Generate diagrams.

# docgen has to be run in the cockroach root directory to find
# the sql.y grammar file.
cd ..

bin/docgen grammar bnf ./generated-diagrams/bnf
bin/docgen grammar svg ./generated-diagrams/bnf ./generated-diagrams/grammar_svg --railroad $railroadPath

cd generated-diagrams

git add .
git commit -m "Snapshot $ref"

github_ssh_key="${PRIVATE_DEPLOY_KEY_FOR_GENERATED_DIAGRAMS}"
configure_git_ssh_key

push_to_git -f ssh://git@github.com/cockroachdb/generated-diagrams.git

common_support_remove_files_on_exit
