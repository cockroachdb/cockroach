#!/usr/bin/env bash

# Detect whether the installed version of Go can build this version of
# CockroachDB.
#
# To bump the required version of Go, edit the last conditional appropriately.
#
# Note: this script is invoked by Make, so it has odd error reporting semantics.
# Errors are printed to stdout; the Go version is compatible iff nothing is
# printed to stdout. The exit code is meaningless.

# Ensure stray error messages are printed to stdout, or they'll go unnoticed by
# Make.
exec 2>&1

go=${1-go}

if ! raw_version=$("$go" version 2>&1); then
  echo "unable to detect go version: $raw_version"
  exit
fi

if ! version=$(grep -oE "[0-9]+\.[0-9]+" <<< "$raw_version" | head -n1); then
  echo "unable to parse go version '$raw_version'"
  exit
fi

version_major=$(cut -f1 -d. <<< "$version")
version_minor=$(cut -f2 -d. <<< "$version")
if (( version_major != 1 )) || (( version_minor < 10 )); then
  echo "go1.10+ required (detected go$version)"
  exit
fi
