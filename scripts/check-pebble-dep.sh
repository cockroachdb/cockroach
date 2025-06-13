#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail
#set -x

# Find all release-xx.y branches where xx >= 24.
BRANCHES=$(git branch -r --format='%(refname)' \
    | grep '^refs\/remotes/\origin\/release-2[4-9]\.[0-9]$' \
    | sed 's/^refs\/remotes\/origin\///' \
    | sort -V)

EXIT_CODE=0
for BRANCH in $BRANCHES; do
  if [ "$BRANCH" = "release-24.2" ]; then
    # Skip the release-24.2 branch, which is frozen.
    continue
  fi
  PEBBLE_BRANCH="crl-$BRANCH"
  DEP_SHA=$(git show "origin/$BRANCH:go.mod" |
    grep 'github.com/cockroachdb/pebble' |
    grep -o -E '[a-f0-9]{12}$')
  TIP_SHA=$(git ls-remote --heads 'https://github.com/cockroachdb/pebble' |
    grep "refs/heads/$PEBBLE_BRANCH" |
    grep -o -E '^[a-f0-9]{12}')
  
  if [ "$DEP_SHA" = "$TIP_SHA" ]; then
    continue
  fi

  if [ $EXIT_CODE -eq 0 ]; then
    echo "Some release branches have out-of-date Pebble dependencies:"
  fi
  echo "  - $BRANCH: dependency set at $DEP_SHA, but $PEBBLE_BRANCH tip is $TIP_SHA"
  EXIT_CODE=1
done

if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi

echo "All release branches have up-to-date Pebble dependencies:"
for BRANCH in $BRANCHES; do
  echo " - $BRANCH"
done
