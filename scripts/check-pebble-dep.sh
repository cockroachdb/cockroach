#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail
#set -x

RELEASES="23.1 23.2 24.1 24.2 24.3 master"

for REL in $RELEASES; do
  if [ "$REL" == "master" ]; then
    BRANCH=master
    PEBBLE_BRANCH=master
  else
    BRANCH="release-$REL"
    PEBBLE_BRANCH="crl-release-$REL"
  fi
  DEP_SHA=$(git show "origin/$BRANCH:go.mod" |
    grep 'github.com/cockroachdb/pebble' |
    grep -o -E '[a-f0-9]{12}$')
  TIP_SHA=$(git ls-remote --heads 'https://github.com/cockroachdb/pebble' |
    grep "refs/heads/$PEBBLE_BRANCH" |
    grep -o -E '^[a-f0-9]{12}')
  
  if [ "$DEP_SHA" == "$TIP_SHA" ]; then
    echo Branch $BRANCH pebble dependency up to date.
    continue
  fi

  echo Branch $BRANCH pebble dependency not up to date: $DEP_SHA vs current $TIP_SHA
  if [ "$BRANCH" == "master" ]; then
    # Do nothing on master.
    # TODO(radu): run scripts/bump-pebble.sh and open PR?
    :
  else
    # File an issue, unless one is filed already.
    TITLE="release-$REL: update pebble dependency"
    if [ $(gh issue list -R github.com/cockroachdb/cockroach --search "$TITLE" --json id) == "[]" ]; then
      echo "Filing issue for release-$REL."
      BODY="Branch dependency is cockroachdb/pebble@$DEP_SHA. Tip of $PEBBLE_BRANCH is cockroachdb/pebble@$TIP_SHA"
      gh issue create -R github.com/cockroachdb/cockroach --title "$TITLE" --body "$BODY" --label T-storage --label A-storage
    else
      echo "Issue for release-$REL already exists."
    fi
  fi
done
