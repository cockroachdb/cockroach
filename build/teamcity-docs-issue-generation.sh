#!/usr/bin/env bash

# Creates issues in cockroachdb/docs repo for each commit with Release Note text not set to "None" or "Bug fix"
# See https://cockroachlabs.atlassian.net/l/c/XRboHcdm

# Vars for local testing
# BUILD_VCS_NUMBER="replace_with_a_cockroach_commit_hash" (for ex. 934c7da83035fb78108daa23fa9cb8925d7b6d10)
# GITHUB_API_TOKEN="replace_with_your_github_api_token"

remove_files_on_exit() {
  rm -f commits.json match.json message messages.txt prnums valid
}
trap remove_files_on_exit EXIT

# A merge may contain multiple PRs due to craig(bot) activity
# Get PR numbers for current merge
curl -s --retry 5 -H "Authorization: token $GITHUB_API_TOKEN" https://api.github.com/search/issues?q=sha:"$BUILD_VCS_NUMBER"+repo:cockroachdb/cockroach | jq .items[].number > prnums

# For each PR, process its commit(s)
process_pr_commits() {
  # Get commits given PR number
  commits=$(curl -s --retry 5 -H "Authorization: token $GITHUB_API_TOKEN" https://api.github.com/repos/cockroachdb/cockroach/pulls/"$1"/commits)
  echo "$commits" > commits.json
  pr_title=$(curl -s --retry 5 -H "Authorization: token $GITHUB_API_TOKEN" https://api.github.com/repos/cockroachdb/cockroach/pulls/"$1" | jq .title)
  commitcount=$( jq length commits.json )
  commitctr=0

  # Iterate through all commit messages on this PR
  while [ "$commitctr" -lt "$commitcount" ]
  do
    jq -r --arg i "$commitctr" '.[$i|tonumber].commit.message' commits.json > message
    # For each commit message, check whether it contains at least one release note
    hasReleaseNotes=$( jq -r --arg i "$commitctr" '.[$i|tonumber].commit.message|test("[rR]elease [nN]ote")' commits.json )
    # Extract the type(s) of release note(s)
    jq -r --arg i "$commitctr" '.[$i|tonumber].commit.message|match("[rR]elease [nN]ote (?<type>.*): .*")' commits.json > match.json
    # Check whether there is at least one release note type that is not none" or "bug"-related
    jq -r '.captures | .[] | .string|test("[nN]one|[bB]ug")' match.json > valid

    # If conditions fulfilled, format the release note(s) and create a docs issue
    if [ "$hasReleaseNotes" = true ] && grep -q false valid
    then
      echo "https://github.com/cockroachdb/cockroach/pull/$1" > messages.txt
      echo "---" >> messages.txt
      # Get Release Note line for issue body
      cat message | sed -n '/[rR]elease [nN]ote/,$p' >> messages.txt
      # Create issue body
      body=$(cat messages.txt)
      # Replace % with %25
      body="${body//'%'/'%25'}"
      # Replace newlines with spaces
      body="${body//$'\n'/' '}"
      # Replace carriage returns with spaces
      body="${body//$'\r'/' '}"
      # Replace backslash with %5C
      body="${body//$'\\'/'%5C'}"
      # Escape double quotes
      body="${body//$'\"'/'\''"'}"
      # Get merge branch
      merge_branch=$(curl -s --retry 5 -s -H "Authorization: token $GITHUB_API_TOKEN" https://api.github.com/repos/cockroachdb/cockroach/pulls/"$1" | jq .base.ref)
      # Create issue
      echo "Creating issue for PR $prnum"
      curl -s --retry 5 -X POST -H "Authorization: token $GITHUB_API_TOKEN" \
        -d '{"title": '"$pr_title"', "labels": ["C-product-change", '"$merge_branch"'], "body": "'"$body"'"}' \
        https://api.github.com/repos/cockroachdb/docs/issues
    fi
    commitctr=$(expr "$commitctr" + 1)
  done
}

# For each PR in the merge, process the PR's associated commits
cat prnums | while read prnum
do
  process_pr_commits "$prnum"
done
