# Common helpers for teamcity-*.sh scripts.

# maybe_ccache turns on ccache to speed up compilation, but only for PR builds
# (i.e., not builds on master or release branches). This speeds up the CI cycle
# for developers while preventing ccache from corrupting a release build.
maybe_ccache() {
  if [[ "$TC_BUILD_BRANCH" != master && "$TC_BUILD_BRANCH" != release-* ]]; then
    export COCKROACH_BUILDER_CCACHE=1
  fi
}
