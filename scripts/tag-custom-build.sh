#!/usr/bin/env bash

# This script makes it easy to make custom builds.
#
# It creates a tag for a SHA that triggers a build in the Make and Publish
# Build TeamCity build config. Once the build is complete, binaries and a
# docker image are available. For details on how to validate everything is
# correct and how to use the binaries/docker image, see:
# https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/846299518/One-Off+Builds+A+How+To
#
# How to use this script:
#
# 1) To tag the checked out SHA (the script is not available for releases
#    v20.1.5, v19.2.10 and older; use option 2 for those releases) run it
#    with no arguments from the root of the repo.
#
# 2) To tag a non-checked out SHA including any SHAs on releases (or branches)
#    older than v20.1.5 and v19.2.10, run it from the root of the repo with
#    the SHA that you want to tag as the single argument.
#
#      ./scripts/tag-custom-build.sh "$SHA"
#
# Note the Tag Name and Build ID (printed at the end of the script output).
#
# Verify the SHA on the GitHub page for the tag (it should open automatically
# in your browser) is the one you tagged. (If the page didn't open in your
# browser, the tag should be somewhere in this list, not necessarily at the top:
# https://github.com/cockroachdb/cockroach/tags .)
#
# Use the tag name to find the build in the Make and Publish Build build config
# in TeamCity.
#
# Use the Build ID when referencing the binaries and docker image with others.

set -euo pipefail

SHA="${1-}"

if [ -z "$SHA" ] ; then
    SHA="$(git rev-parse HEAD)"
fi

# Ensure all the latest tags are downloaded locally
git fetch -t

ID="$(git describe --tags --match=v[0-9]* "$SHA")"
TAG="custombuild-$ID"

git tag "$TAG" "$SHA"
git push git@github.com:cockroachdb/cockroach.git "$TAG"

TAG_URL="https://github.com/cockroachdb/cockroach/releases/tag/${TAG}"
TEAMCITY_URL="https://teamcity.cockroachdb.com/buildConfiguration/Internal_Release_MakeAndPublishBuild?branch=${TAG}&mode=builds"
if [ "$(command -v open)" ] ; then
    open "$TEAMCITY_URL"
    open "$TAG_URL"
elif [ "$(command -v xdg-open)" ] ; then
    xdg-open "$TEAMCITY_URL"
    xdg-open "$TAG_URL"
fi

cat << EOF

See the one-off builds wiki page for steps for the rest of the process:

  https://cockroachlabs.atlassian.net/wiki/spaces/ENG/pages/846299518/One-Off+Builds+A+How+To

Here is the tag in GitHub:

  $TAG_URL

Here is where the build run should show up in TeamCity for the tag:

  $TEAMCITY_URL

Tag name: $TAG
Build ID: $ID
EOF
