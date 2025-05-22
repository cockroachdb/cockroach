#!/usr/bin/env bash
#
# This script helps understand changes in pgo-induced optimizations between two
# source profiles.
#
# Usage:
# - edit the `pgo_profile` in WORKSPACE with a local file:// url to the (local)
#   updated profile and comment out the sha256.
# - add changes and commit
# - run this script.
# - inspect pgodiff/* and in particular `pgo.diff`.
#
# The script first builds a pgo-enabled cockroach-short binary (with pgo debug
# output enabled), then repeats the process on the previous commit.
#
# It greps both sets of debug outputs for inlining budget increases and
# devirtualization decisions, sorts them, and generates a diff.  The diff'ed
# items are not categorized by potential impact. A direct comparison between
# the profiles via `pprof -diff_base old.pb.gz -normalize new.pb.gz` is likely
# a better first avenue of investigation, but in the absence of obvious changes
# on provides, the diffs could point at changes in inlining and virtualization
# decisions.
#
# Note: understanding why an updated profile meaningfully regresses performance
# will likely require a significant time investment. This script does not provide
# answers. Perhaps most of its value lies in documenting how pgo observability
# artifacts can be obtained.
set -euo pipefail

function pgobuild() {
  bazel build //pkg/cmd/cockroach-short --config=pgo --@io_bazel_rules_go//go/config:gc_goopts="-d=pgodebug=3,-env=foobar=$(date +%s)" 2>&1
}

function pgogrep() {
  grep -E 'hot-node enabled increased budget|PGO devirtualizing' "$@"
}

mkdir -p pgodiff

pgobuild > pgodiff/new.txt

git checkout HEAD~
trap "git checkout -" EXIT

pgobuild > pgodiff/old.txt

pgogrep new.txt | sort > pgodiff/new.grep
pgogrep old.txt | sort > pgodiff/old.grep

diff pgodiff/{old,new}.grep > pgodiff/pgo.diff
