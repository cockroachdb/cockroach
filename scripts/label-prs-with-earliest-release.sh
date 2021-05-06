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

for sha in $REF_LIST; do
	# We get earliest tag based on a refs
	GIT_VERSION=$(git name-rev --tags --name-only $sha)
    # TODO: sometimes the command above returns `undefined`
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
    # TODO: what do we do with tags that don't match `v.*`?
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
    http_code=$(curl --retry 5 \
        -w "%{http_code}" -o /dev/null \
		-s -f -X POST https://api.github.com/repos/cockroachdb/cockroach/labels \
        -H "Authorization: token $(cat ${GH_TOKEN_PATH})" -d '{"name":"'"${LABEL}"'","color":"000000"}' || true)

    case $http_code in
        201 | 422)
            ;;
        *)
            echo "Failed to create ${LABEL}, HTTP status code: ${http_code}"
            exit 1
            ;;
    esac

    curl --retry 5 \
		 -s -f -X POST https://api.github.com/repos/cockroachdb/cockroach/issues/${PRNUMBER}/labels \
		 -H "Authorization: token $(cat ${GH_TOKEN_PATH})" \
		 -d '["'"${LABEL}"'"]'
    echo "Label ${LABEL} added to PR #${PRNUMBER}"
done
