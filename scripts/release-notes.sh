#!/usr/bin/env bash

set -euo pipefail

range=${1?specify range, e.g. v2.0-alpha.20171218...1b2eabdd856263662cef7f0850ad74ef669cb2d9}

git log $range --pretty='%H %s' --no-merges | (
    nonotes=
    counter=0
    while read commit title; do
	message=$(git log $commit -n 1 --pretty='%B')
	if echo "$message" | grep -qE '^[rR]elease notes?: *[Nn]one'; then
	    # Explicitly no release note. Nothing to do.
	    continue
	fi
	notes=$(echo "$message" | grep -A 10000 -E '^[rR]elease note[^:]*:' || true)
	if test -z "$notes"; then
	    # Missing 'release note' line. Keep track for later.
	    nonotes=$(echo "$nonotes"; echo "$commit $title")
	    continue
	fi
	echo "$commit $title"
	echo "$notes" | sed -e 's/^/    /g'
	echo
	counter=$(expr $counter + 1)
    done
    echo "The following commits did not include release notes (not even 'none'):"
    echo "$nonotes"
    echo
    echo "Total: $counter release note(s) found."
)
