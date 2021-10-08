#!/usr/bin/env bash

# Detect whether the installed version of Go can build this version of
# CockroachDB.
#
# To bump the required version of Go, edit the appropriate variables:

required_version_major=1
minimum_version_minor=16
minimum_version_16_patch=5

go=${1-go}

if ! raw_version=$("$go" version 2>&1); then
  echo "unable to detect go version: $raw_version" >&2
  exit 1
fi

if ! version=$(grep -oE "[0-9]+(\.[0-9]+)+" <<< "$raw_version" | head -n1); then
  echo "unable to parse go version '$raw_version'" >&2
  exit 1
fi

version_major=$(cut -f1 -d. <<< "$version")
version_minor=$(cut -f2 -d. <<< "$version")
version_patch=$(cut -f3 -d. <<< "$version")
required_version_patch=$(eval echo \$minimum_version_${version_minor}_patch)
check_patch=$(if test -n "$version_patch"; then echo 1; else echo 0; fi)
if (( version_major != required_version_major )) || \
     (( version_minor < minimum_version_minor )); then
  echo "go$required_version_major.$minimum_version_minor+ required (detected go$version)" >&2
  exit 1
elif (( check_patch == 1 && version_patch < required_version_patch )); then
  minimum_version_patch=$(eval echo \$minimum_version_${minimum_version_minor}_patch)
  echo "need Go patch $required_version_major.$version_minor.$required_version_patch+ when using go$required_version_major.$version_minor (detected go$version; minimum version for successful builds is go$required_version_major.$minimum_version_minor.$minimum_version_patch+)" >&2
  exit 1
fi
