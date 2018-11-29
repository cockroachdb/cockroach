#!/bin/sh
set -eux

. common.sh

t=test7
relnotescript=${1:?}
rewrite=${2:-}

test_init

(
    cd $t
    init_repo

	git checkout -b release-branch
    git checkout -b feature1
    make_change "feature A

Release note (bug fix): feature A
"
	make_change "feature B

Release note (bug fix): feature B
"
    tag_pr 1
    git checkout master
    merge_pr feature1 1 "PR title 1"

    git checkout release-branch -b backport
    make_change "backport A

Release note (bug fix): feature A
"
    tag_pr 2
    git checkout release-branch
    merge_pr backport 2 "PR title 2"
)

test_end --exclude-from initial --exclude-until release-branch
