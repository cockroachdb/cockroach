#!/bin/sh
set -eux

. common.sh

t=test4
relnotescript=${1:?}
rewrite=${2:-}

test_init

(
    cd $t
    init_repo
    git checkout -b feature

    make_change "feature A

Release note (bug fix): feature A release note 1

some text for note 1

Release note (bug fix): feature A release note 2

some text for note 2
"

    make_change "feature B

Release note (bug fix, core change): feature B release note 1
"

    make_change "feature C

Release note (bug fix): feature C

Fixes #123124. (this should not be included in result)
close #12313. (this neither)
"

    # See other authors.
    make_change "feature D

Release note (bug fix): feature D
"
    git commit --amend --date="$date" --author='foo <foo@example.com>' --no-edit --allow-empty

    make_change "feature E

Release note (bug fix): feature E

Co-authored-by: bar <bar@example.com>

trailing garbage (should be ignored)
"

    tag_pr 1
    git checkout master

    merge_pr feature 1 "PR title"
)

test_end
