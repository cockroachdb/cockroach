#!/usr/bin/env bash

# Detect whether the installed version of Go can build this version of
# CockroachDB.
#
# To bump the required version of Go, edit the last conditional appropriately.

go=${1-go}

required_version_major=1
required_version_minor=11

if ! raw_version=$("$go" version 2>&1); then
  echo "unable to detect go version: $raw_version" >&2
  exit 1
fi

if ! version=$(grep -oE "[0-9]+\.[0-9]+" <<< "$raw_version" | head -n1); then
  echo "unable to parse go version '$raw_version'" >&2
  exit 1
fi

version_major=$(cut -f1 -d. <<< "$version")
version_minor=$(cut -f2 -d. <<< "$version")
if (( version_major != required_version_major )) || (( version_minor < required_version_minor )); then
  echo "go$required_version_major.$required_version_minor+ required (detected go$version)" >&2
  exit 1
fi
