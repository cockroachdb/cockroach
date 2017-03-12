#!/usr/bin/env bash

# buildinfo.sh manages tag and revision information in both proper git checkouts
# and source tarballs without a .git directory.
#
# When invoked as `buildinfo.sh tag` or `buildinfo.sh rev`, the current tag or
# current commit SHA, respectively, is printed to stdout. buildinfo.sh will
# prefer the information cached in the .buildinfo directory, if the directory
# exists, and will otherwise query Git directly. The script will fail if the
# source tree has neither a .buildinfo cache nor a Git directory.
#
# Executing `buildinfo.sh inject-archive` will write tag and revision
# information into the .buildinfo directory, and stage that directory in the git
# index, so that it can later be found by `buildinfo.sh tag` or `buildinfo.sh
# ref`.

set -eu

cd "$(dirname "$0")"/..

discover_tag() {
    tag="$(git describe --tags --exact-match 2> /dev/null || git rev-parse --short HEAD)"
    git diff-index --quiet HEAD || tag+=-dirty
    echo "$tag"
}

discover_rev() {
    git rev-parse HEAD
}

write_stdin_to_index() {
    git update-index --add --cacheinfo "100644,$(git hash-object -w --stdin),$1"
}

if [[ "$1" = tag ]]
then
    [[ -f .buildinfo/tag ]] && exec cat .buildinfo/tag
    discover_tag
elif [[ "$1" = rev ]]
then
    [[ -f .buildinfo/rev ]] && exec cat .buildinfo/rev
    discover_rev
elif [[ "$1" = inject-archive ]]
then
    discover_tag | write_stdin_to_index .buildinfo/tag
    discover_rev | write_stdin_to_index .buildinfo/rev
else
    echo "$0: unknown command $1" >&2
    exit 1
fi
