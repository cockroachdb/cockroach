#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


BASE_SHA="$1"
HEAD_SHA="$2"

if [ -z "$HEAD_SHA" ];then
    echo "Usage: $0 <base-sha> <head-sha>"
    exit 1
fi

git diff --name-only "${BASE_SHA}..${HEAD_SHA}" -- "pkg/**/*.go" ":!*/testdata/*" ":!pkg/acceptance/compose/gss/psql/**" \
  | xargs -rn1 dirname \
  | sort -u \
  | { while read path; do if ls "$path"/*.go &>/dev/null; then echo -n "$path "; fi; done; }
