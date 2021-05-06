#!/usr/bin/env bash

# This script tag a PR with the earliest version that contains it
# to avoid manual handling and time taking.

# Sample usage:
#		$ git-tag <PullRequestNumber> <Commit Sha>
#
# If the scripts shows: gh: Validation Failed (HTTP 422)
# it is expected and the tag will be only attached and not created
# even if we use silent, we cannot avoid this message to come from API.

set -euo pipefail

PRNUM=$1
COMMITSHA=$2

GIT_VERSION=$(git tag --contains ${COMMITSHA})

# COMMENT="""
# Automated comment to show tag version from the commits.
# **********************
# - Commit 08d4a089ffe33676a0fd421e024e244cbd1957fa
# - tag version: ${GIT_VERSION}
# **********************
# """

# gh pr comment 1 --body "${COMMENT}"

LABEL="earliest-release-v${GIT_VERSION}"
# First we need to create the label
if gh api --silent repos/alan-mas/stringer_test/labels -f name=${LABEL} -f color="000000"
then
	echo "New Tag ${LABEL} created"
	gh pr edit ${PRNUM} --add-label ${LABEL}
	echo "New Tag ${LABEL} added to PR: ${PRNUM}"
else 
	# In case label is already created, it will only tag de PR.
	echo "Tag ${LABEL} already exists"
	gh pr edit ${PRNUM} --add-label ${LABEL}
	echo "Tag added to PR: ${PRNUM}"
fi

# GET /repos/:owner/:repo/pulls/:pull_number/commits
# curl \
#   -H "Accept: application/vnd.github.v3+json" \
#   https://api.github.com/repos/alan-mas/stringer_test/pulls/1/commits
