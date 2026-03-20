#!/bin/bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Finds and cleans up stale bazel output bases in commonly used locations.

# Optionally also deduplicates content of files across active/recently-used
# output bases.

set -euo pipefail

STALE_BASE_DAYS=14      # days since last use before removing an output base
KNOWN_OUTPUT_ROOTS=("${HOME}/.cache/bazel/_bazel_${USER}" "/private/var/tmp/_bazel_${USER}")

# Exit immediately if not running on darwin.
# TODO(dt): support linux hosts.
if [[ "$(uname)" != "Darwin" ]]; then
  echo "tidy_bazel_bases.sh is currently only supported on macOS." >&2
  exit 1
fi

# Exit early if another instance is already running. We use noclobber to
# atomically create the PID file, avoiding a TOCTOU race between checking and
# writing.
PIDFILE="${HOME}/.cache/bazel-tidy/pid"
mkdir -p "$(dirname "$PIDFILE")"
if [ -f "$PIDFILE" ]; then
  owner=$(cat "$PIDFILE" 2>/dev/null || echo "")
  if [ -n "$owner" ] && ps -p "$owner" >/dev/null 2>&1; then
    echo "Another instance of tidy_bazel_bases.sh is already running (PID $owner). Exiting." >&2
    exit 1
  fi
  # Stale PID file from a dead process; remove it so noclobber can proceed.
  rm -f "$PIDFILE"
fi
if ! (set -C; echo "$$" > "$PIDFILE") 2>/dev/null; then
  echo "Another instance of tidy_bazel_bases.sh started concurrently. Exiting." >&2
  exit 1
fi
trap 'rm -f "$PIDFILE"' EXIT

# maybe_remove_base checks if a base is stale, and removes it if so.
# Returns 0 if the base was removed, 1 otherwise.
maybe_remove_base() {
  local base="$1"

  local age workspace
  # Bazel writes DO_NOT_BUILD_HERE with the workspace path on first use; we can
  # use that to check if the workspace still exists and remove the stale base if
  # not.
  workspace=$(cat "${base}/DO_NOT_BUILD_HERE" 2>/dev/null || echo "")
  if [ -n "$workspace" ] && [ ! -d "$workspace" ]; then
    echo "Removing stale base (workspace $workspace gone): $base"
    rm -rf -- "$base"
    return 0
  fi

  # Bazel writes to the output base directory so its mtime is a last-used signal.
  # NB: stat -f is macOS-specific; see the Darwin guard at the top of this script.
  local mtime
  if ! mtime=$(stat -f %m "$base" 2>/dev/null); then
    return 0
  fi
  age=$(( ($(date +%s) - mtime) / 86400 ))
  if [ "$age" -gt "$STALE_BASE_DAYS" ]; then
    echo "Removing stale base (unused ${age}d): $base"
    rm -rf -- "$base"
    return 0
  fi

  echo "Keeping base $base: last used ${age}d ago, workspace: ${workspace:-unknown}." >&2
  return 1
}

# find_bases discovers bazel output bases by searching for DO_NOT_BUILD_HERE
# marker files that bazel writes in every output base. Outputs one base path
# per line.
find_bases() {
  for candidate in "${KNOWN_OUTPUT_ROOTS[@]}"; do
    [ -d "$candidate" ] || continue
    # Default layout: <root>/<hash>/DO_NOT_BUILD_HERE
    for f in "$candidate"/*/DO_NOT_BUILD_HERE; do
      [ -f "$f" ] && dirname "$f"
    done
    # Multi-base layout: <root>/bases/<suffix>/<N>/DO_NOT_BUILD_HERE
    for f in "$candidate"/bases/*/*/DO_NOT_BUILD_HERE; do
      [ -f "$f" ] && dirname "$f"
    done
  done
}

# cull_stale_bases finds all bazel bases and removes those that are stale.
cull_stale_bases() {
  local bases=()
  while IFS= read -r line; do
    bases+=("$line")
  done < <(find_bases)

  echo "Found ${#bases[@]} bazel output bases. Checking for stale ones..."
  for base in "${bases[@]}"; do
    maybe_remove_base "$base" || true
  done
}

# dedupe_base_contents deduplicates file content across surviving bases using
# APFS reflinks via fclones.
dedupe_base_contents() {
  # Opt-in: dedup only runs if the user has created ~/.cache/bazel-tidy/dedupe
  # and fclones is installed.
  if ! command -v fclones >/dev/null 2>&1 || [ ! -d "${HOME}/.cache/bazel-tidy/dedupe" ]; then
    return
  fi

  # NB: We may act on a base concurrently with bazel writing to it: the chmod
  # could miss files as bazel adds them, which would prevent de-duping (which is
  # fine -- we'll get them next run), or bazel could add more files after the
  # scan has read a folder -- also fine for the same reason. There is a tiny
  # window during de-dupe where a file could appear to be missing which could
  # cause a concurrent build to fail. If this proves to be a problem we can make
  # the script more complicated by acquiring the bazel lock in a base while
  # de-duping it.
  local bases=()
  while IFS= read -r line; do
    bases+=("$line")
  done < <(find_bases)

  if [ ${#bases[@]} -lt 2 ]; then
    return
  fi

  echo "Running fclones dedupe across ${#bases[@]} bases..."

  # Group duplicates and deduplicate via reflinks, using persistent cache.
  if ! nice fclones group --cache "${bases[@]}" > "${HOME}/.cache/bazel-tidy/dedupe/dupes.txt"; then
    echo "fclones group failed." >&2
    return
  fi

  dupe_count=$(grep -c '^    ' "${HOME}/.cache/bazel-tidy/dedupe/dupes.txt") || dupe_count=0
  if [ "$dupe_count" -eq 0 ]; then
    echo "No duplicates found across bases."
    return
  fi
  echo "Found $dupe_count duplicate files across bases."

  # Extract unique parent directories of duplicate files and make them writable
  # so fclones can rename files during reflink.
  sed -n 's|^    \(.*\)/[^/]*$|\1|p' "${HOME}/.cache/bazel-tidy/dedupe/dupes.txt" \
    | sort -u | tr '\n' '\0' | xargs -0 nice chmod u+w 2>/dev/null || true

  echo "Performing deduplication with fclones..."
  # NB: If bazel has com.apple.provenance set (i.e. if bazelisk had it set when
  # it was downloaded and thus bazel binaries inherited it), then files made by
  # bazel also have it set, including the files in the output bases. When fclones
  # deduplicates files, it creates new files and tries to copy the xattrs, but
  # the com.apple.provenance xattr is not settable by non-root, so fclones will
  # fail. DT's patched version of fclones adds --ignore-xattr-errors to handle
  # this gracefully; use it if available.
  local dedupe_flags="--no-lock"
  if fclones dedupe --help 2>&1 | grep -q 'ignore-xattr-errors'; then
    dedupe_flags="--no-lock --ignore-xattr-errors"
  fi
  nice fclones dedupe $dedupe_flags < "${HOME}/.cache/bazel-tidy/dedupe/dupes.txt" || true

  echo "fclones dedup complete."
}

cull_stale_bases
dedupe_base_contents
