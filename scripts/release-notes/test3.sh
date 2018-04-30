#!/bin/sh
set -eux

. common.sh

t=test3
relnotescript=${1:?}
rewrite=${2:-}

test_init

(
    cd $t
    init_repo
    git branch feature
    make_change "master update"
    git checkout feature
    make_change "feature A

Release note (bug fix): feature A
"
    git branch feature2
    make_change "feature B

Release note (bug fix): feature B
"
    git checkout feature2
    make_change "feature C

Release note (bug fix): feature C
"
    git checkout feature
    merge_branch feature2

    make_change "feature D

Release note (bug fix): feature D
"
    tag_pr 1
    git checkout master
    merge_pr feature 1 "PR title"
)

test_end

