#!/bin/sh
set -eux

. common.sh

t=test6
relnotescript=${1:?}
rewrite=${2:-}

test_init

(
    cd $t
    init_repo

    git checkout -b feature1
    make_change "feature A

Release note: none
"
    tag_pr 1
    git checkout master
    merge_pr feature1 1 "PR title 1"

    git checkout -b feature2
    make_change "feature B - commit 1"
    make_change "feature B - commit 2

Release note: none
"
    tag_pr 2
    git checkout master
    merge_pr feature2 2 "PR title 2"
)

test_end
