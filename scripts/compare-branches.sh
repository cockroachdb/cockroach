#!/bin/bash
set -euo pipefail

if [ "$#" == "0" ]; then
	echo "usage: $0 <base> <branch> [git args...]"
    echo ""
    echo "print commits on <base> since merge-base with branch that are not cherrypicked to <branch>"
    echo "extra args are passed though to git log e.g. --author=jsmith or -n 100"
	exit 1
fi

base="$1"
shift
branch="$1"
shift

from="$(git merge-base $base $branch)"

base_log="$(mktemp)"
branch_log="$(mktemp)"

git log --no-merges "$from..$base" --pretty=format:'%s @ %ai	%an	https://github.com/cockroachdb/cockroach/commit/%h ' "$@" \
           | sort > "$base_log"
git log --no-merges "$from..$branch" --pretty=format:'%s @ %ai	%an	https://github.com/cockroachdb/cockroach/commit/%h ' "$@" \
    | sort > "$branch_log"

# NB: head -n 100 | sort catches a SIGPIPE, so don't use that.
# http://www.pixelbeat.org/programming/sigpipe_handling.html

join -t '@' -v 1 "$base_log" "$branch_log" |
    awk 'BEGIN { FS="@"; } {print $2,$1}' |
    sort
