#!/bin/sh
set -eux

. common.sh

t=test10
relnotescript=${1:?}
rewrite=${2:-}

test_init

(
    cd $t
    init_repo
    git checkout -b feature
    make_change "feature A"
	tag_pr 1
	git checkout master
	
    make_change "master update

Release note (bug fix): some fix.
"
)

test_end
