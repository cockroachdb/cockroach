#!/usr/bin/env bash

# import.sh updates C and C++ dependencies from their upstream sources and
# applies CockroachDB patches. Usage:
#
#     ./import.sh [DEP...]
#
# If dependency specifications are omitted, all dependencies will be updated.
#
# To add a new dependency, add it to the `deps` array below. To update a
# dependency, update the URL with the new version in the `deps` array and run
# `./import.sh DEP`. Patch files in this directory of the form DEP-*.patch are
# automatically applied after the dependency is downloaded and extracted.

set -euo pipefail
shopt -s nullglob

((${BASH_VERSION%%.*} >= 4)) || {
  echo "fatal: bash 4 or later required. You have $BASH_VERSION." >&2
  exit 1
}

declare -A deps
deps=(
    [jemalloc]=https://github.com/jemalloc/jemalloc/releases/download/4.5.0/jemalloc-4.5.0.tar.bz2
    # v3.2.1 accidentally uses C++11 features that are removed on head.
    # v3.2.2 should be safe.
    # See: https://github.com/google/protobuf/issues/2769.
    [protobuf]=https://github.com/google/protobuf/archive/v3.2.0.tar.gz
    [rocksdb]=https://github.com/facebook/rocksdb/archive/v5.1.4.tar.gz
    [snappy]=https://github.com/google/snappy/releases/download/1.1.3/snappy-1.1.3.tar.gz
)

mangle_protobuf() {
  rm -r protobuf.src/examples
}

(($# >= 1)) && goals=("$@") || goals=("${!deps[@]}")

for dep in "${goals[@]}"; do
  [[ "${deps["$dep"]:-}" ]] || {
    echo "unrecognized dep $dep" >&2
    exit 1
  }
done

for dep in "${goals[@]}"; do
  echo "> updating $dep"
  url="${deps[$dep]}"
  rm -rf "$dep.src"
  mkdir -p "$dep.src"
  curl -sfSL "$url" | tar --strip-components=1 -C "$dep.src" -x
  for patch in "$dep"-*.patch; do
    echo ">> $patch"
    patch -d "$dep.src" -p1 < "$patch"
  done
  type -t "mangle_$dep" > /dev/null && set -x && "mangle_$dep" && set +x
  # TODO(benesch): it would be good for these tarballs to be reproducible.
  # Currently, any patches cause them not to be, because of the change in
  # mtime. Fix this.
  touch -mt 200001010000 "$dep.src"
  echo ">> generating compressed tarball"
  tar -cJf "$dep.src.tar.xz" "$dep.src"
done

