#!/usr/bin/env bash

# This script tag a PR with the earliest official release the commit is part of.
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

# As we need to get the earliest official release the commit is part of,
# we need to use only the earliest v.* tag with the exception of skipping v*-alpha.00000000 tags
# in favor of the first alpha or beta tag.
# Also we are ignoring the rare cases in which tag name is undefined.
# eg: git name-rev --tags b2bf0a0ef6


FindTag() {
    # $1 git SHA value
    VERSION_TAGS=$(git tag --contains $1 | sort | grep -E '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)')
	ADD_LABEL="true"

	ALPHA_00_REGEX='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-alpha.00+$'
	ALPHA_BETA_RC_REGEX='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-[-.0-9A-Za-z]+$'
	PATCH_REGEX='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.[1-9][0-9]*$'
	MAJOR_RELEASE_REGEX='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.0$'

	if $(echo "$VERSION_TAGS" | sed -n 1p | grep -qE "$ALPHA_00_REGEX") ; then
    	# A *-alpha.00000000 tag is first
    	# if there is a second entry, use it. Otherwise skip this PR/commit.
    	if [ $(echo "$VERSION_TAGS" | wc -l) -gt 1 ] ; then
        	VERSION_TO_SET=$(echo "$VERSION_TAGS" | sed -n 2p)
    	else
    		echo "Skipping PR $1 as *-alpha.00000000 tag is first"
        	ADD_LABEL="false"
    	fi
	elif $(echo "$VERSION_TAGS" | sed -n 1p | grep -qE "$ALPHA_BETA_RC_REGEX") ; then
    	# An alpha/beta/rc tag is first, use the first entry.
    	VERSION_TO_SET=$(echo "$VERSION_TAGS" | sed -n 1p)
	elif $(echo "$VERSION_TAGS" | sed -n 1p | grep -qE "$PATCH_REGEX") ; then
    	# A vX.Y.Z patch release >= .1 is first (ex: v20.1.1).
    	VERSION_TO_SET=$(echo "$VERSION_TAGS" | sed -n 1p)
	elif $(echo "$VERSION_TAGS" | sed -n 1p | grep -qE "$MAJOR_RELEASE_REGEX") ; then
    	# A vX.Y.0 release.
	    if [ $(echo "$VERSION_TAGS" | wc -l) -gt 2 ] && $(echo "$VERSION_TAGS" | sed -n 2p | grep -qE "$ALPHA_00_REGEX") ; then
	        # This commit was in a *-alpha.00000000 tag so take the next tag after it (an alpha or a beta; it could be good to validate the next tag is indeed one of those).
	        VERSION_TO_SET=$(echo "$VERSION_TAGS" | sed -n 3p)
	    elif [ $(echo "$VERSION_TAGS" | wc -l) -gt 1 ] && $(echo "$VERSION_TAGS" | sed -n 2p | grep -qE "$ALPHA_00_REGEX") ; then
	         # This shouldn't be reached. It might be nice to alert if this happens.
	         echo "Skipping PR $1 as *-alpha.00000000 should not be reached"
	         ADD_LABEL=false
	    elif [ $(echo "$VERSION_TAGS" | wc -l) -gt 1 ] && ! $(echo "$VERSION_TAGS" | sed -n 2p | grep -qE "$ALPHA_00_REGEX")  && $(echo "$VERSION_TAGS" | sed -n 2p | grep -qE "$ALPHA_BETA_RC_REGEX") ; then
	        # It's an alpha/beta/rc.
	        VERSION_TO_SET=$(echo "$VERSION_TAGS" | sed -n 2p)
	    else
	        # It's the vX.Y.0 release.
	        VERSION_TO_SET=$(echo "$VERSION_TAGS" | sed -n 1p)
	    fi
	fi
}



for sha in $REF_LIST; do
	# Get earliest tag.
	FindTag "$sha"
	# Need to confirm that PR has a valid version to tag.
	if [ "$ADD_LABEL" == 'true' ] ; then
	    # Creating version label string
		LABEL="earliest-release-${VERSION_TO_SET}"
		# Getting PR number to use when calling GitHub API with curl in next if condition.
		PRNUMBER=$(git show --oneline --format="format:%h %s" $sha | head -n1 | sed -n "s/^.*Merge pull request #\\s*\\([0-9]*\\).*$/\\1/p")
		echo "SHA: $sha earliest version: $VERSION_TO_SET label: $LABEL pr: $PRNUMBER"

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
	fi
done
