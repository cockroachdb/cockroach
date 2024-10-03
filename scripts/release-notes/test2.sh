#!/bin/sh

# Copyright 2018 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -eux

. common.sh

t=test2
relnotescript=${1:?}
rewrite=${2:-}

test_init

# Initialize the repo and populate it.
(
    cd $t
    init_repo
    git branch feature
    make_change "master update"
    git checkout feature
    make_change "feature A

Release note (bug fix): feature A
"
    merge_branch master
    make_change "feature B

Release note (bug fix): feature B
"

    tag_pr 1
    git checkout master
    merge_pr feature 1 "PR title"
)

test_end
