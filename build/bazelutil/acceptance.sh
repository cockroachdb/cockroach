# This is boilerplate taken directly from
#  https://github.com/bazelbuild/bazel/blob/master/tools/bash/runfiles/runfiles.bash
# See that page for an explanation of what this is and why it's necessary.
# --- begin runfiles.bash initialization v2 ---
# Copy-pasted from the Bazel Bash runfiles library v2.
set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v2 ---

PACKAGE=pkg/acceptance
NAME=acceptance_test
set -x

# Wrap rlocation such that we immediately fail if a dep is not found.
rlocation_ck () {
    loc="$(rlocation $1)"
    if [ -z "${loc-}" ]; then
	echo "error: could not find the location of $1" >&2
	exit 1
    fi
    echo $loc
}

go_bin="$(rlocation_ck go_sdk/bin/go)"

# Need to run this so that Go can find the runfiles.
runfiles_export_envvars

if [ -z "${BUILD_WORKSPACE_DIRECTORY-}" ]; then
  echo "error: BUILD_WORKSPACE_DIRECTORY not set" >&2
  exit 1
fi

# cd "$BUILD_WORKSPACE_DIRECTORY/$PACKAGE"

TEST_WORKSPACE=cockroach PATH="$(dirname $go_bin):$PATH" GOROOT="$(dirname $(dirname $go_bin))" \
    artifacts/acceptance_test -b artifacts/cockroach-short "$@"
