#!/bin/sh
set -eux

. common.sh

t=test5
relnotescript=${1:?}
rewrite=${2:-}

test_init

(
    cd $t
    init_repo
    git checkout -b feature
    make_change "feature A

Release note (bug fix): feature A
"
    make_change "minor fix - no release note"
    tag_pr 1
    git checkout master
    merge_pr feature 1 "PR title"

    git branch -D feature
    git checkout -b feature
    make_change "feature B - missing release note"
    tag_pr 2
    git checkout master
    merge_pr feature 2 "PR title in need of release note"

)

test_end
