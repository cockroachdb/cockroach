#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Signs, verifies and (optionally) notarizes one extracted macOS release
# directory. The directory holds the product executable at its top level and,
# for `cockroach`, the GEOS shared libraries under lib/:
#
#   cockroach-v26.3.0.darwin-11.0-arm64/
#     cockroach
#     lib/libgeos.dylib
#     lib/libgeos_c.dylib
#
# The executable is signed with the hardened runtime, which turns on library
# validation: a dylib it dlopen()s must be an Apple platform library or carry
# the same Team ID as the executable. The GEOS libraries are therefore signed
# with the same certificate, and all signed files are submitted together for
# notarization so that quarantined downloads pass Gatekeeper.
#
# The signatures are only valid as long as nobody rewrites the files
# afterwards: install_name_tool, strip and Homebrew's post-install relocation
# all invalidate a signature. Packaging that re-distributes these files must
# copy them unchanged.
#
# This script is shared by the GitHub Actions and TeamCity signing jobs and is
# also runnable by hand with a self-signed certificate; see the environment
# variables below.
#
# usage: sign-macos-release-dir.sh DIR PRODUCT P12_FILE P12_PASSWORD_FILE (API_KEY_FILE | --skip-notarize)
#
# Environment:
#   MACOS_SIGN_EXTRA_ARGS
#     Extra arguments appended to every `rcodesign sign` invocation, e.g.
#     "--timestamp-url none" for an offline run. Unset in CI. (Deliberately
#     not prefixed RCODESIGN_: rcodesign reads such variables as config.)
#   LOCAL_TEST_SIGNING
#     When set to 1, the script is being run by hand with a self-signed
#     certificate. The executable is then signed without the `runtime` flag,
#     and the Team ID check reads the signing certificate rather than the
#     code directory: a Team ID is only recorded in (and trusted from) the
#     code directory for certificates that chain to Apple, so with a
#     self-signed certificate library validation can never pass and a
#     hardened executable could not load the libraries at all. Never set in
#     CI.

set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: $0 DIR PRODUCT P12_FILE P12_PASSWORD_FILE (API_KEY_FILE | --skip-notarize)" >&2
  exit 1
fi

dir="$1"
product="$2"
p12_file="$3"
p12_password_file="$4"
api_key_file="$5"

# shellcheck source=build/shlib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../shlib.sh"

if ! command -v rcodesign >/dev/null; then
  echo "rcodesign not found in PATH" >&2
  exit 1
fi

executable="$dir/$product"
if [[ ! -f "$executable" ]]; then
  echo "executable not found: $executable" >&2
  exit 1
fi

# cockroach-sql ships no libraries, so an absent lib/ is fine.
libraries=()
if [[ -d "$dir/lib" ]]; then
  for lib in "$dir"/lib/*.dylib; do
    [[ -f "$lib" ]] && libraries+=("$lib")
  done
fi

# shellcheck disable=SC2206
extra_args=(${MACOS_SIGN_EXTRA_ARGS:-})

# Empty arrays are expanded as ${arr[@]+"${arr[@]}"} so that the script also
# runs under `set -u` on macOS's bash 3.2, which treats an empty array as unset.
sign() {
  local path="$1"
  shift
  rcodesign sign \
    --p12-file "$p12_file" --p12-password-file "$p12_password_file" \
    "$@" ${extra_args[@]+"${extra_args[@]}"} \
    "$path"
}

# The hardened runtime flag is what notarization expects on every Mach-O
# file. On a dylib it has no behavioral effect (library validation is a
# property of the process, set by the executable), but the notary service
# checks for it on all submitted binaries.
for lib in ${libraries[@]+"${libraries[@]}"}; do
  echo "Signing $lib..."
  sign "$lib" --code-signature-flags runtime
done

local_test="${LOCAL_TEST_SIGNING:-}"

echo "Signing $executable..."
if [[ "$local_test" == "1" ]]; then
  echo "WARNING: LOCAL_TEST_SIGNING=1: signing without the hardened runtime; testing only"
  sign "$executable"
else
  sign "$executable" --code-signature-flags runtime
fi

# Verify what was produced rather than trusting the exit codes: every file
# must carry a non-ad-hoc signature with a Team ID, the Team ID must be the
# same on all of them (library validation), and the executable must have the
# runtime flag.
signature_field() {
  local path="$1"
  local field="$2"
  rcodesign print-signature-info "$path" | sed -n "s/^ *${field}: *//p" | head -n1
}

# team_id_of prints the Team ID that library validation will compare: the one
# in the code directory. Under LOCAL_TEST_SIGNING the code directory has none
# (see above), so the leaf certificate's Team ID is reported instead.
team_id_of() {
  local path="$1"
  if [[ "$local_test" == "1" ]]; then
    rcodesign print-signature-info "$path" \
      | grep -A1 'apple_certificate_profile:' \
      | sed -n 's/^ *apple_team_id: *//p' | head -n1
  else
    signature_field "$path" team_name
  fi
}

expected_team_id="$(team_id_of "$executable")"
if [[ -z "$expected_team_id" ]]; then
  echo "verification failed: $executable has no Team ID (ad-hoc, unsigned, or a certificate that does not chain to Apple?)" >&2
  exit 1
fi
echo "Team ID: $expected_team_id"

executable_flags="$(signature_field "$executable" flags)"
if [[ "$local_test" != "1" && "$executable_flags" != *RUNTIME* ]]; then
  echo "verification failed: $executable lacks the hardened runtime flag: $executable_flags" >&2
  exit 1
fi

for path in "$executable" ${libraries[@]+"${libraries[@]}"}; do
  flags="$(signature_field "$path" flags)"
  if [[ "$flags" == *ADHOC* ]]; then
    echo "verification failed: $path is ad-hoc signed: $flags" >&2
    exit 1
  fi
  team_id="$(team_id_of "$path")"
  if [[ "$team_id" != "$expected_team_id" ]]; then
    echo "verification failed: $path Team ID '$team_id' differs from executable Team ID '$expected_team_id'" >&2
    exit 1
  fi
  echo "Verified $path: team=$team_id flags=$flags"
done

if [[ "$api_key_file" == "--skip-notarize" ]]; then
  echo "Skipping notarization."
  exit 0
fi

# Notarize all signed files in one submission. The zip is built from the
# parent directory so that the archive paths stay relative, as the notary
# service requires.
zip_dir="$(mktemp -d)"
trap 'rm -rf "$zip_dir"' EXIT
zip_file="$zip_dir/crl.zip"
base="$(basename "$dir")"
zip_members=("$base/$product")
for lib in ${libraries[@]+"${libraries[@]}"}; do
  zip_members+=("$base/lib/$(basename "$lib")")
done
(cd "$(dirname "$dir")" && zip "$zip_file" "${zip_members[@]}")

echo "Submitting for notarization..."
retry rcodesign notary-submit \
  --api-key-file "$api_key_file" \
  --wait \
  "$zip_file"
echo "Notarization complete."
