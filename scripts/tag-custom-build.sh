#!/usr/bin/env bash

# This script makes it easy to make custom builds.
#
# It creates a tag for a SHA that triggers a build in the Make and Publish Build
# TeamCity build config. Once the build is complete, binaries and a docker
# image are available. For details on accessing them, see:
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

if [ "$(git status -s --untracked-files=no)" ] ; then
    echo "This script only works on clean checkouts. Please stash your local changes first."
    exit 1
fi

ORIG_SHA="$(git rev-parse HEAD)"
ORIG_REF="$(git rev-parse --abbrev-ref HEAD)"
if [ "$ORIG_REF" = "HEAD" ] ; then
    ORIG_REF="$ORIG_SHA"
fi

if [ -z "$SHA" ] ; then
    SHA="$ORIG_SHA"
fi

# Ensure all the latest tags are downloaded locally
git fetch -t

# Switch to the provided SHA?
if [ "$SHA" != "$ORIG_SHA" ] ; then
    git checkout "$SHA"
fi

ID="$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null)"
TAG="custombuild-$ID"

git tag "$TAG" "$SHA"
git push git@github.com:cockroachdb/cockroach.git "$TAG"

# Switch back to the original ref?
if [ "$SHA" != "$ORIG_SHA" ] ; then
    git checkout "$ORIG_REF"
fi

TAG_URL="https://github.com/cockroachdb/cockroach/releases/tag/${TAG}"
if [ "$(command -v open)" ] ; then
    open "$TAG_URL"
elif [ "$(command -v xdg-open)" ] ; then
    xdg-open "$TAG_URL"
fi

echo
echo "Tag name: $TAG"
echo "Build ID: $ID"
