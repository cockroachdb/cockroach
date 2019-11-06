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
    definitely_ccache
  fi
}

definitely_ccache() {
  run export COCKROACH_BUILDER_CCACHE=1
}

run() {
  echo "$@"
  "$@"
}

# Returns the list of release branches from origin (origin/release-*), ordered
# by version (bigger version numbers first).
get_release_branches() {
  git branch -r --format='%(refname)' \
    | sed 's/^refs\/remotes\///' \
    | grep 'origin\/release-*' \
    | sort -t- -k2 -g -r
}

# Returns the number of commits in the curent branch that are not shared with
# the given branch.
get_branch_distance() {
  git rev-list --count $1..HEAD
}

# Returns the closest branch between origin/master and origin/release-* branches.
get_upstream_branch() {
  local UPSTREAM DISTANCE D

  UPSTREAM="origin/master"
  DISTANCE=$(get_branch_distance origin/master)

  # Check if we're closer to any release branches. The branches are ordered
  # new-to-old, so stop as soon as the distance starts to increase.
  for branch in $(get_release_branches); do
    D=$(get_branch_distance $branch)
    if [ $D -gt $DISTANCE ]; then
      break
    fi
    UPSTREAM=$branch
    DISTANCE=$D
  done

  echo "$UPSTREAM"
}

changed_go_pkgs() {
  git fetch --quiet origin
  upstream_branch=$(get_upstream_branch)
  # Find changed packages, minus those that have been removed entirely.
  git diff --name-only "$upstream_branch..." -- "pkg/**/*.go" ":!*/testdata/*" \
    | xargs -rn1 dirname \
    | sort -u \
    | { while read path; do if ls "$path"/*.go &>/dev/null; then echo -n "./$path "; fi; done; }
}

tc_release_branch() {
  [[ "$TC_BUILD_BRANCH" == master || "$TC_BUILD_BRANCH" == release-* || "$TC_BUILD_BRANCH" == provisional_* ]]
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
