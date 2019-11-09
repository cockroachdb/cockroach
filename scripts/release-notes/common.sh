#!/bin/sh

# Initialize a test.
function test_init() {
    PYTHON=${PYTHON:-python}

    # We fix all the git per-commit parameters so that the repo structure
    # becomes deterministic.

    date="Sun Apr 22 19:26:11 2018 +0200"
    author="$t <$t@example.com>"

    export GIT_COMMITTER_DATE=$date
    export GIT_COMMITTER_NAME=$t
    export GIT_COMMITTER_EMAIL=$t@example.com
    flags=(--date="$date" --author="$author")

    rm -rf $t
    mkdir $t
}

# Initialize the repository. Tag the initial commit.
function init_repo() {
    git init
    touch foo; git add foo; git commit "${flags[@]}" -m "initial"; git tag initial
	git tag v000-base
}

# Perform some arbitrary change.
# $1 = commit message.
function make_change() {
    git commit --allow-empty "${flags[@]}" -m "$1"
}

# Mark a branch tip as PR tip.
# $1 = PR number.
function tag_pr() {
    mkdir -p .git/refs/pull/origin
    git log --pretty=tformat:%H > .git/refs/pull/origin/$1
}

# Merge a regular branch into the current branch.
# $1 = branch name
function merge_branch() {
    git merge --no-ff -m "Merge $1 into current" $1
    git commit --amend --no-edit "${flags[@]}"
}

# Merge a PR branch into the current branch.
# $1 = branch name
# $2 = PR number
# $3 = PR title
function merge_pr() {
    branchname=$1
    prnum=$2
    prtitle=$3
    git merge --no-ff -m "Merge #$prnum

$prnum: $prtitle r=foo a=bar

Release note (core change): this must not be included.

Co-authored-by: invisible <invisible@example.com>
" $branchname
    git commit --amend --no-edit "${flags[@]}"

    # Make a dummy second pr just to compare the PR message.
    git checkout -b dummypr
    make_change "merge pr canary

Release note: none
"
    prnum=$(expr $prnum \* 100)
    tag_pr $prnum

    git checkout master
    git merge --no-ff -m "Merge pull request #$prnum from foo/bar

$prtitle alternate format
" dummypr
    git commit --amend --no-edit "${flags[@]}"
    git branch -D dummypr
}

function test_end() {
    # Check the repo layout is correct.
    (cd $t && git log --graph --pretty=oneline) >$t.graph.txt
    if test -z "$rewrite"; then
        diff -u $t.graph.txt $t.graph.ref.txt
    else
        mv -f $t.graph.txt $t.graph.ref.txt
    fi

    # Check the generated release notes.
    (cd $t && $PYTHON $relnotescript --hide-header --hide-downloads-section --from initial --until master "$@") >$t.notes.txt
    if test -z "$rewrite"; then
        diff -u $t.notes.txt $t.notes.ref.txt
    else
        mv -f $t.notes.txt $t.notes.ref.txt
    fi
    rm -rf $t
}
