# Common helpers for teamcity-*.sh scripts.

# root is the absolute path to the root directory of the repository.
root=$(cd "$(dirname "$0")/.." && pwd)

# maybe_ccache turns on ccache to speed up compilation, but only for PR builds.
# This speeds up the CI cycle for developers while preventing ccache from
# corrupting a release build.
maybe_ccache() {
  if tc_release_branch; then
    echo "On release branch ($TC_BUILD_BRANCH), so not enabling ccache."
  else
    echo "Building PR (#$TC_BUILD_BRANCH), so enabling ccache."
    run export COCKROACH_BUILDER_CCACHE=1
  fi
}

run() {
  echo "$@"
  "$@"
}

changed_go_pkgs() {
  git fetch --quiet origin master
  # Find changed packages, minus those that have been removed entirely.
  git diff --name-only origin/master... -- "pkg/**/*.go" \
    | xargs -rn1 dirname \
    | sort -u \
    | { while read path; do if ls "$path"/*.go &>/dev/null; then echo -n "./$path "; fi; done; }
}

tc_release_branch() {
  [[ "$TC_BUILD_BRANCH" == master || "$TC_BUILD_BRANCH" == release-* ]]
}

tc_start_block() {
  echo "##teamcity[blockOpened name='$1']"
}

if_tc() {
  if [[ "${TC_BUILD_ID-}" ]]; then
    "$@"
  fi
}

tc_end_block() {
  echo "##teamcity[blockClosed name='$1']"
}

tc_prepare() {
  tc_start_block "Prepare environment"
  run export BUILDER_HIDE_GOPATH_SRC=1
  run mkdir -p artifacts
  maybe_ccache
  tc_end_block "Prepare environment"
}
