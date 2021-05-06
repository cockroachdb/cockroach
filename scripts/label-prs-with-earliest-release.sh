#!/usr/bin/env bash

# This script tag a PR with the earliest version that contains it
# to avoid manual handling and time taking.

# Sample usage:
#		$ label-prs-with-earliest-release.sh <PullRequestNumber> <Commit Sha>

set -euo pipefail

# 1. Create your github PERSONAL ACCESS TOKEN at https://github.com/settings/tokens

# 2. Enter these fields
GH_TOKEN_PATH=          # path to your PERSONAL ACCESS TOKEN


# We need to get all the merged PR in a time window
START_SHA=$1
STOP_SHA=$2


REF_LIST=$(git log --merges --reverse --oneline --ancestry-path "${START_SHA}..${STOP_SHA}" | grep "Merge pull request" | cut -c-10)

for sha in $REF_LIST; do
	# We get earliest tag based on a refs
	GIT_VERSION=$(git name-rev --tags --name-only $sha)
	# there are some numvers coming after the wave, need to check why is this happening
	# Remove everything coming after ~ character
	GIT_VERSION_CLEAN=$(echo "$GIT_VERSION" | cut -f1 -d"~")
	# Creating version label string
	LABEL="earliest-release-${GIT_VERSION_CLEAN}"
	# Getting PR number to use it with GitHub API
	PRNUMBER=$(git show --oneline $sha | head -n1 | sed -n "s/^.*Merge pull request #\\s*\\([0-9]*\\).*$/\\1/p")
	echo "SHA: $sha earliest version: $GIT_VERSION_CLEAN label: $LABEL pr: $PRNUMBER"

	# Based on GITHUB API documentation:
	# Every pull request is an issue, but not every issue is a pull request.
	# For this reason, "shared" actions for both features, like manipulating assignees, labels
	# and milestones, are provided within the Issues API.
	# That is why we are using issues path instead of pull request in the https link
	# https://docs.github.com/en/rest/reference/pulls#update-a-pull-request

	# First we need to create the label.
	if curl \
		-s -f -X POST https://api.github.com/repos/cockroachdb/cockroach/labels \
		-H "Authorization: token $(cat ${GH_TOKEN_PATH})" -d '{"name":"'"${LABEL}"'","color":"000000"}'
	then
		echo "New Tag ${LABEL} created"
		curl -s -f -X POST https://api.github.com/repos/cockroachdb/cockroach/issues/${PRNUM}/labels \
			 -H "Authorization: token $(cat ${GH_TOKEN_PATH})" \
			 -d '["'"${LABEL}"'"]'
		echo "New Tag ${LABEL} added to PR: ${PRNUM}"
	else 
		# In case label is already created, it will only tag de PR.
		echo "Tag ${LABEL} already exists"
		curl -s -f -X POST https://api.github.com/repos/cockroachdb/cockroach/issues/${PRNUM}/labels \
			   -H "Authorization: token $(cat ${GH_TOKEN_PATH})" \
			   -d '["'"${LABEL}"'"]'
		echo "Tag added to PR: ${PRNUM}"
	fi
done

