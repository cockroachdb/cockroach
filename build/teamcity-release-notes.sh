#!/usr/bin/env bash

remove_files_on_exit() {
  rm -f commits.json match.json message messages.txt prnums valid 
}
trap remove_files_on_exit EXIT

# A merge may contain multiple PRs due to craig(bot) activity
# Get PR numbers for current merge
curl -H 'Authorization: token $GITHUB_API_TOKEN' https://api.github.com/search/issues?q=sha:$BUILD_VCS_NUMBER+repo:cockroachdb/cockroach | jq .items[].number > prnums

# For each PR, process its commit(s)
process_pr_commits() {
  # Get commits given PR number
  commits=$(curl -H 'Authorization: token $GITHUB_API_TOKEN' https://api.github.com/repos/cockroachdb/cockroach/pulls/$1/commits)
  commits="${commits//$'\''/'%27'}"
  echo $commits > commits.json
  pr_title=$(curl -H 'Authorization: token $GITHUB_API_TOKEN' https://api.github.com/repos/cockroachdb/cockroach/pulls/$1 | jq .title)
  commitcount=$( jq length commits.json )
  commitctr=0

  # Iterate through all commit messages on this PR
  while [ $commitctr -lt $commitcount ]
  do
    jq -r --arg i "$commitctr" '.[$i|tonumber].commit.message' commits.json > message
    # For each commit message, check whether it contains at least one release note
    bool=$( jq -r --arg i "$commitctr" '.[$i|tonumber].commit.message|test("[rR]elease [nN]ote")' commits.json )
    # Extract the type(s) of release note(s)
    jq -r --arg i "$commitctr" '.[$i|tonumber].commit.message|match("[rR]elease [nN]ote (?<type>.*): .*")' commits.json > match.json
    # Check whether there is at least one release note type that is not none" or "bug"-related
    jq -r '.captures | .[] | .string|test("[nN]one|[bB]ug")' match.json > valid

    # If conditions fulfilled, format the release note(s) and create a docs issue
    if $bool == true && grep -q false valid
    then
      echo "https://github.com/cockroachdb/cockroach/pull/$1" > messages.txt
      echo "---" >> messages.txt
      cat message | sed -n '/[rR]elease [nN]ote/,$p' >> messages.txt
      body=$(cat messages.txt)
      body="${body//'%'/'%25'}"
      body="${body//$'\n'/' '}"
      body="${body//$'\r'/' '}"
      body="${body//$'\\'/'%5C'}"
      echo $body
      # Get merge branch
      merge_branch=$(curl -H 'Authorization: token $GITHUB_API_TOKEN' https://api.github.com/repos/cockroachdb/cockroach/pulls/$1 | jq .base.ref)
      curl -X POST -H 'Authorization: token $GITHUB_API_TOKEN' \
        -d '{"title": "'"$pr_title"'", "labels": ["C-product-change", "'"$merge_branch"'"], "body": "'"$body"'"}' \
        https://api.github.com/repos/cockroachdb/docs/issues
    fi
    commitctr=`expr $commitctr + 1`
  done
}

# For each PR in the merge, process the PR's associated commits
cat prnums | while read prnum
do
  process_pr_commits $prnum
done
