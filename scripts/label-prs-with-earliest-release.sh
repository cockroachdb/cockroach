#!/usr/bin/env bash

# This script tag a PR with the earliest version that contains it
# to avoid manual handling and time taking.

# Sample usage:
#		$ label-prs-with-earliest-release.sh <path to your PERSONAL ACCESS TOKEN> <oldest git-ref> <earliest git-ref>

set -euo pipefail

# 1. Create your github PERSONAL ACCESS TOKEN at https://github.com/settings/tokens
	# - Please add all the `repo` scope on this token.
	# - https://docs.github.com/en/developers/apps/building-oauth-apps/scopes-for-oauth-apps

# 2. Enter these fields
# path to your PERSONAL ACCESS TOKEN
GH_TOKEN_PATH=$1


# We need to get all the merged PR in a time window
# START_SHA: Should point to the oldest git-ref you want to check.
# STOP_SHA: Should point to the earliest git-ref you want to check.
START_SHA=$2
STOP_SHA=$3

# As we want to get all the log information based in 2 different git-ref within the git history, we are using
# git log command as it was the easiest way to accomplish this.
# We are using the below git log arguments:
# --merges: As we only wants to fetch (or print) merge commits.
# --reverse: Show the choosen commits in reverse order (By default, the commits are shown in reverse chronological order).
# --oneline: Shorthand for "--pretty=oneline --abbrev-commit" used together. Only shows prefix and uses 80 characters.
# --ancestry-path: Only display commits that exist directly on the ancestry chain between the 2 SHA references.
# --format: Using explicitly, so we don't rely on the default git log format. https://git-scm.com/docs/git-log#_pretty_formats.
# https://git-scm.com/docs/git-log

REF_LIST=$(git log --merges --reverse --oneline --format="format:%h %s" --ancestry-path "${START_SHA}..${STOP_SHA}" | grep "Merge pull request" | cut -d ' ' -f1)


# Curl function to DRY within creating label
curlCall(){
    curl --retry 5 \
		 -s -f -X POST https://api.github.com/repos/cockroachdb/cockroach/issues/$1/labels \
		 -H "Authorization: token $(cat $2)" \
		 -d '["'"$3"'"]'
}

for sha in $REF_LIST; do
	# We get earliest tag based on a refs
	GIT_VERSION=$(git name-rev --tags --name-only $sha)
	# there are some numbers coming after the wave, need to check why is this happening
	# eg:
	# ```
	# $ git name-rev --tags --name-only 58061b6c28
	# v21.1.0-beta.2~62
	# $ git name-rev --tags --name-only 6373a476f8
	# v21.1.0-beta.2~61
	# ```
	# Remove everything coming after ~ character
	GIT_VERSION_CLEAN=$(echo "$GIT_VERSION" | cut -f1 -d"~")
	# Creating version label string
	LABEL="earliest-release-${GIT_VERSION_CLEAN}"
	# Getting PR number to use when calling GitHub API with curl in next if condition.
	PRNUMBER=$(git show --oneline --format="format:%h %s" $sha | head -n1 | sed -n "s/^.*Merge pull request #\\s*\\([0-9]*\\).*$/\\1/p")
	echo "SHA: $sha earliest version: $GIT_VERSION_CLEAN label: $LABEL pr: $PRNUMBER"

	# Based on GITHUB API documentation:
	# Every pull request is an issue, but not every issue is a pull request.
	# For this reason, "shared" actions for both features, like manipulating assignees, labels
	# and milestones, are provided within the Issues API.
	# That is why we are using issues path instead of pull request in the https link
	# https://docs.github.com/en/rest/reference/pulls#update-a-pull-request

	# First we need to create the label.
	if curl --retry 5 \
		-s -f -X POST https://api.github.com/repos/cockroachdb/cockroach/labels \
		-H "Authorization: token $(cat ${GH_TOKEN_PATH})" -d '{"name":"'"${LABEL}"'","color":"000000"}'
	then
		echo "New Tag ${LABEL} created"
		curlCall ${PRNUMBER} ${GH_TOKEN_PATH} ${LABEL}
		echo "New Tag ${LABEL} added to PR: ${PRNUMBER}"
	else 
		# In case label is already created, it will only tag de PR.
		echo "Tag ${LABEL} already exists"
		curlCall ${PRNUMBER} ${GH_TOKEN_PATH} ${LABEL}
		echo "Tag added to PR: ${PRNUMBER}"
	fi
done

