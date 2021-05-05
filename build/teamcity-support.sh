# Common helpers for teamcity-*.sh scripts.

# root is the absolute path to the root directory of the repository.
root=$(cd "$(dirname "$0")/.." && pwd)

source "$root/build/teamcity-common-support.sh"

remove_files_on_exit() {
  rm -f ~/.ssh/id_rsa{,.pub}
  common_support_remove_files_on_exit
}
trap remove_files_on_exit EXIT

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

run_counter=-1

# Takes args that produce `go test -json` output. It filters stdout to contain
# only test output related to failing tests and run/pass/skip events for the
# other tests (no output). It writes artifacts/failures.txt containing text
# output for the failing tests.
# It's valid to call this multiple times; all output artifacts will be
# preserved.
function run_json_test() {
  run_counter=$((run_counter+1))
  tc_start_block "prep"
  # TODO(tbg): better to go through builder for all of this.
  go install github.com/cockroachdb/cockroach/pkg/cmd/github-post
  mkdir -p artifacts
  tmpfile="artifacts/raw.${run_counter}.json.txt"
  tc_end_block "prep"

  tc_start_block "run"
  set +e
  run "$@" 2>&1 \
    | tee "${tmpfile}" \
    | (cd "$root"/pkg/cmd/testfilter && go run main.go -mode=strip) \
    | tee artifacts/stripped.txt
  status=$?
  set -e
  tc_end_block "run"

  # Post issues, if on a release branch. Note that we're feeding github-post all
  # of the build output; it also does some slow test analysis.
  if tc_release_branch; then
    if [ -z "${GITHUB_API_TOKEN-}" ]; then
      # GITHUB_API_TOKEN must be in the env or github-post will barf if it's
      # ever asked to post, so enforce that on all runs.
      # The way this env var is made available here is quite tricky. The build
      # calling this method is usually a build that is invoked from PRs, so it
      # can't have secrets available to it (for the PR could modify
      # build/teamcity-* to leak the secret). Instead, we provide the secrets
      # to a higher-level job (Publish Bleeding Edge) and use TeamCity magic to
      # pass that env var through when it's there. This means we won't have the
      # env var on PR builds, but we'll have it for builds that are triggered
      # from the release branches.
      echo "GITHUB_API_TOKEN must be set"
      exit 1
    else
      tc_start_block "post issues"
      github-post < "${tmpfile}"
      tc_end_block "post issues"
    fi
  fi

  tc_start_block "artifacts"
  # Create (or append to) failures.txt artifact and delete stripped.txt.
  (cd "$root"/pkg/cmd/testfilter && go run main.go -mode=omit) < artifacts/stripped.txt | \
      (cd "$root"/pkg/cmd/testfilter && go run main.go -mode=convert) >> artifacts/failures.txt

  if [ $status -ne 0 ]; then
    # Keep the debug file around for failed builds. Compress it to avoid
    # clogging the agents with stuff we'll hopefully rarely ever need to
    # look at.
    # If the process failed, also save the full human-readable output. This is
    # helpful in cases in which tests timed out, where it's difficult to blame
    # the failure on any particular test. It's also a good alternative to poking
    # around in $tmpfile itself when anything else we don't handle well happens,
    # whatever that may be.
    fullfile=artifacts/full_output.txt
    (cd "$root"/pkg/cmd/testfilter && go run main.go -mode=convert) < "${tmpfile}" >> "${fullfile}"
    tar --strip-components 1 -czf "${tmpfile}.tgz" "${tmpfile}" "${fullfile}"
    rm -f "${fullfile}"
  fi
  rm -f "${tmpfile}" artifacts/stripped.txt
  tc_end_block "artifacts"

  # Make it easier to figure out whether we're exiting because of a test failure
  # or because of some auxiliary failure.
  tc_start_block "exit status"
  echo "test run finished with exit status $status"
  tc_end_block "exit status"
  return $status
}

function maybe_stress() {
  # Don't stressrace on the release branches; we only want that to happen on the
  # PRs. There's no need in making master flakier than it needs to be; nightly
  # stress will weed out the flaky tests.
  # NB: as a consequence of the above, this code doesn't know about posting
  # Github issues.
  if tc_release_branch; then
    return 0
  fi

  target=$1
  shift

  block="Maybe ${target} pull request"
  tc_start_block "${block}"
  run build/builder.sh go install ./pkg/cmd/github-pull-request-make
  run_json_test build/builder.sh env BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" TARGET="${target}" github-pull-request-make
  tc_end_block "${block}"
}

# Returns the list of release branches from origin (origin/release-*), ordered
# by version (higher version numbers first).
get_release_branches() {
  # We sort by the minor version first, followed by a stable sort on the major
  # version.
  git branch -r --format='%(refname)' \
    | sed 's/^refs\/remotes\///' \
    | grep '^origin\/release-*' \
    | sort -t. -k2 -n -r \
    | sort -t- -k2 -n -r -s
}

# Returns the number of commits in the curent branch that are not shared with
# the given branch.
get_branch_distance() {
  git rev-list --count $1..HEAD
}

# Returns the branch among origin/master, origin/release-* which is the
# closest to the current HEAD.
#
# Suppose the origin looks like this:
#
#                e (master)
#                |
#                d       w (release-19.2)
#                |       |
#                c       u
#                 \     /
#                  \   /
#                   \ /
#                    b
#                    |
#                    a
#
# Example 1. PR on master on top of d:
#
#      e (master)   pr
#             \     /
#              \   /
#               \ /
#                d       w (release-19.2)
#                |       |
#                c       u
#                 \     /
#                  \   /
#                   \ /
#                    b
#                    |
#                    a
#
# The pr commit has distance 1 from master and distance 3 from release-19.2
# (commits c, d, and pr); so we deduce that the upstream branch is master.
#
# Example 2. PR on release-19.2 on top of u:
#
#                e (master)
#                |
#                d   w (release-19.2)
#                |     \
#                |      \   pr
#                |       \ /
#                c       u
#                 \     /
#                  \   /
#                   \ /
#                    b
#                    |
#                    a
#
# The pr commit has distance 2 from master (commits u and w) and distance 1 from
# release-19.2; so we deduce that the upstream branch is release-19.2.
#
# If the PR is on top of the fork point (b in the example above), we return the
# release-19.2 branch.
#
# Example 3. PR on even older release:
#
#                e (master)
#                |
#                d    w (release-19.2)
#                |       |
#                |       |
#                |       |        pr
#                c       u       /
#                 \     /       y (release-19.1)
#                  \   /       /
#                   \ /       /
#                    b       x
#                     \     /
#                      \   /
#                       \ /
#                        a
#
# The pr commit has distance 3 from both master and release-19.2 (commits x, y,
# pr) and distance 1 from release-19.1. In general, the distance w.r.t. all
# newer releases than the correct one will be equal; specifically, it is the
# number of commits since the fork point of the correct release (the fork point
# in this example is commit a).
#
get_upstream_branch() {
  local UPSTREAM DISTANCE D

  UPSTREAM="origin/master"
  DISTANCE=$(get_branch_distance origin/master)

  # Check if we're closer to any release branches. The branches are ordered
  # new-to-old, so stop as soon as the distance starts to increase.
  for branch in $(get_release_branches); do
    D=$(get_branch_distance $branch)
    # It is important to continue the loop if the distance is the same; see
    # example 3 above.
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
  # Find changed packages, minus those that have been removed entirely. Note
  # that the three-dot notation means we are diffing against the merge-base of
  # the two branches, not against the tip of the upstream branch.
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

generate_ssh_key() {
  if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
    ssh-keygen -q -N "" -f ~/.ssh/id_rsa
  fi
}
