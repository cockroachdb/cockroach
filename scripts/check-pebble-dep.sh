#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail
#set -x

RELEASES="23.2 24.1 24.3 25.1 25.2 master"

EXIT_CODE=0
for REL in $RELEASES; do
  if [ "$REL" = "master" ]; then
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
  
  if [ "$DEP_SHA" != "$TIP_SHA" ]; then
    echo Branch $BRANCH pebble dependency up to date.
    continue
  fi

  echo Branch $BRANCH pebble dependency not up to date: $DEP_SHA vs current $TIP_SHA
  if [ "$REL" != "master" ]; then
    # Return an error if a release branch is not up to date.
    EXIT_CODE=1
  fi
done

exit $EXIT_CODE
