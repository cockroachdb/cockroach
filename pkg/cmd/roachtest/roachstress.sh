#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

# This script is an opinionated helper around building binaries and artifacts.
# It is particularly helpful for stressing roachtests at multiple revisions.
#
# This script is to be invoked from the repository root checked out at the revision
# to be tested.
#
# It's best practice to invoke this script with "caffeinate" on OSX and/or linux
# to avoid computer going to standby.

if [ -z "${BASH_VERSINFO}" ] || [ -z "${BASH_VERSINFO[0]}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  echo "Error: This script requires Bash version >= 4"
  echo "Error: On OSX, 'brew install bash' should do the trick."
  exit 1
fi

function help {
  cat <<EOF
Build and stress run roachtest
Usage:
  $(basename "${0}") [flags] <test name> [-- <roachtest flags>]

flags:
  -c COUNT - number of test iterations to run
  -l       - run test locally on host
  -u       - build cockroach with ui (don't use buildshort)
  -b       - do not rebuild binaries if they exist

Roachstress will build artifacts inside artifacts/<commit-hash> directory
for later reuse. This is eliminating the need to rebuild when comparing
branches.
If roachstress detects that there's an uncommitted change, it would always
try to rebuild into artifacts/<commit-hash>-dirty to allow temporary test
or cockroach changes for reproductions.
Local change rebuilding behavior could be disabled by -b flag.
EOF
}

function fail {
  echo -e "Error: $1\n"
  help
  exit 1
}

# Process command line flags
force_build=
short=-short
local=
count=10
while getopts ":c:lubh" o ; do
  case "$o" in
    c)
     count="${OPTARG}"
     ;;
    l)
     local=.local
     ;;
    u)
     short=
     ;;
    b)
     force_build=n
     ;;
    h)
     help
     exit 0
     ;;
    :)
     fail "Option -${OPTARG} requires argument"
     ;;
    *)
     fail "Unknown option -${OPTARG}"
     ;;
  esac
done

# Some magic to ensure test name is present. We need special handling for the case where
# test name was not provided but -- and roachtest arguments were. getopts makes it hard
# to distinguish since cases where OPTIND points to test name or OPTIND points to the first
# arg to roachtest look identical.

# Check if we parsed beyond last arg already, then its an error.
[ $# -lt "${OPTIND}" ] && fail "No test name provided"

# We are at arg, but it may be test name or first roachtest arg.
if [ $OPTIND -gt 1 ] ; then
  # Check if arg to the left is '--' then we don't have test name.
  shift $((OPTIND - 2))
  [ "$1" = "--" ] && fail "No test name provided"
  shift 1
fi

test="$1"
# Finally extract roachprod params if present.
shift 1
if [ $# -gt 0 ] ; then
  [ "$1" != "--" ] && fail "Can only provide single test name, did you forget -- before roachtest arguments"
  shift 1
fi

# Define the artifacts base dir, within which both the built binaries and the
# artifacts will be stored.
sha=$(git rev-parse --short HEAD)
# If local changes are detected use separate artifacts dir and force rebuild.
if [ -n "$(git status --porcelain --untracked-files=no)" ] ; then
  sha="${sha}-dirty"
  force_build=${force_build:-y}
fi
abase="artifacts/${sha}"
mkdir -p "${abase}"
trap 'echo Build artifacts dir is ${abase}' EXIT

# Locations of the binaries.
rt="${abase}/roachtest"
wl="${abase}/workload${local}"
cr="${abase}/cockroach${local}"

# This is the artifacts dir we'll pass to the roachtest invocation. It's
# disambiguated by a timestamp because one often ends up invoking roachtest on
# the same SHA multiple times and artifacts shouldn't mix.
timestamp=$(date '+%H%M%S')
a="${abase}/${timestamp}"
ln -fs "${sha}/${timestamp}" "artifacts/latest"

if [ ! -f "${cr}" ] || [ "${force_build}" = "y" ]; then
  if [ -z "${local}" ]; then
    ./dev build "cockroach${short}" --cross=linux
    cp "artifacts/cockroach${short}" "${cr}"
  else
    ./dev build "cockroach${short}"
    cp "cockroach${short}" "${cr}"
  fi
fi

if [ ! -f "${wl}" ] || [ "${force_build}" = "y" ]; then
  if [ -z "${local}" ]; then
    ./dev build workload --cross=linux
    cp "artifacts/workload" "${wl}"
  else
    ./dev build workload
    cp "bin/workload" "${wl}"
  fi
  ./dev build roachtest
  cp "bin/roachtest" "${rt}"
fi

# Creation of test data directory is deferred here to avoid spamming in
# cases where build fails.
mkdir -p "${a}"
trap 'echo Find run artifacts in ${a}' EXIT

args=(
  "run" "${test}"
  "--port" "$((8080+RANDOM % 1000))"
  "--prom-port" "$((2113+RANDOM % 1000))"
  "--workload" "${wl}"
  "--cockroach" "${cr}"
  "--artifacts" "${a}"
  "--count" "${count}"
)
if [ -n "${local}" ]; then
  args+=("--local")
fi
args+=("$@")

echo "Running ${rt} " "${args[@]}"
# Run roachtest. Use a random port so that multiple
# tests can be stressed from the same workstation.
"${rt}" "${args[@]}"
