#!/bin/sh
set -eux

. common.sh

t=test8
relnotescript=${1:?}
rewrite=${2:-}

test_init

(
    cd $t
    init_repo

	cat >AUTHORS <<EOF
Foo Foo <foo@example.com> foo <foo@example.com> <foo2@example.com>
EOF
	git add AUTHORS
	make_change "update AUTHORS"

    git checkout -b feature
    make_change "feature A1"
	git commit --allow-empty --amend --author='foo <foo@example.com>' --no-edit
	tag_pr 1
	git checkout master
	merge_pr feature 1 "PR 1 title"

	git checkout -b feature2
	make_change "feature A2"
	git commit --allow-empty --amend --author='foo <foo2@example.com>' --no-edit
	tag_pr 2
	git checkout master
	merge_pr feature2 2 "PR 2 title"
)

test_end
