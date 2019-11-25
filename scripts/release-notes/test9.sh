#!/bin/sh
set -eux

. common.sh

t=test9
relnotescript=${1:?}
rewrite=${2:-}

test_init

(
    cd $t
    init_repo

    git checkout -b feature
    make_change "fix 1

Release note (bug fix): something wrong!
"
	make_change "feature 2

Release note (cli change): badly explained
"
	last_sha=$(git log -n 1 --pretty=%H)
	tag_pr 1
	git checkout master
	merge_pr feature 1 "PR 1 title"

	git checkout -b feature2
	make_change "note revision

Release note: None
Revised release note [#1] (bug fix): something is fixed.
The explanation can even be lengthy.
Revised release note [#1] (cli change): This is better explained.
"
	make_change "note revision

Revised release note [#1] (cli change): Wait I think this is even
better.

Revised release note [$last_sha] (cli change): One more detail.
"

	make_change "synthetized note

Revised release note [#1] (extra): This release note did not
exist before.
"
	last_sha=$(git log -n 1 --pretty=%H)
	make_change "synthetized note

Revised release note [#2] (cli change): This was missing.
Revised release note [$last_sha] (extra): This was also missing.
"
	tag_pr 2
	git checkout master
	merge_pr feature2 2 "PR 2 title"
	git tag noteend

	git checkout -b feature3
	make_change "extra revisions

Revised release note [#2] (cli change): This is visible.

Revised release note [#3] (extra): This must not be visible
because it is not selected for the release note report.
"
	tag_pr 3
	git checkout master
	merge_pr feature3 3 "PR 3 title"
)

test_end --until noteend
