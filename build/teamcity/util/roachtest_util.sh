# Common logic used by the nightly roachtest scripts (Bazel and non-Bazel).

# Set up Google credentials. Note that we need this for all clouds since we upload
# perf artifacts to Google Storage at the end.
if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

# Early bind the stats dir. Roachtest invocations can take ages, and we want the
# date at the time of the start of the run (which identifies the version of the
# code run best).
stats_dir="$(date +"%Y%m%d")-${TC_BUILD_ID}"

# Set up a function we'll invoke at the end.
function upload_stats {
 if tc_release_branch; then
      bucket="${ROACHTEST_BUCKET:-cockroach-nightly-${CLOUD}}"
      if [[ "${CLOUD}" == "gce" ]]; then
          # GCE, having been there first, gets an exemption.
          bucket="cockroach-nightly"
      fi

      branch=$(tc_build_branch)
      remote_artifacts_dir="artifacts-${branch}"
      if [[ "${branch}" == "master" ]]; then
        # The master branch is special, as roachperf hard-codes
        # the location.
        remote_artifacts_dir="artifacts"
      fi
      # In FIPS-mode, keep artifacts separate by using the 'fips' suffix.
      if [[ ${FIPS_ENABLED:-0} == 1 ]]; then
        remote_artifacts_dir="${remote_artifacts_dir}-fips"
      fi

      # The stats.json files need some path translation:
      #     ${artifacts}/path/to/test/stats.json
      # to
      #     gs://${bucket}/artifacts/${stats_dir}/path/to/test/stats.json
      #
      # `find` below will expand "{}" as ./path/to/test/stats.json. We need
      # to bend over backwards to remove the `./` prefix or gsutil will have
      # a `.` folder in ${stats_dir}, which we don't want.
      (cd "${artifacts}" && \
        while IFS= read -r f; do
          if [[ -n "${f}" ]]; then
            gsutil cp "${f}" "gs://${bucket}/${remote_artifacts_dir}/${stats_dir}/${f}"
          fi
        done <<< "$(find . -name stats.json | sed 's/^\.\///')")
  fi
}

# Upload any stats.json we can find, no matter what happens.
trap upload_stats EXIT

# Set up the parameters for the roachtest invocation.
PARALLELISM=16
CPUQUOTA=1024
TESTS="${TESTS-}"
FILTER="${FILTER-}"
case "${CLOUD}" in
  gce)
      # Confusing due to how we've handled tags in the past where it has been assumed that all tests should
      # be run on GCE. Now with refactoring of how tags are handled, we need:
      # - "default" to ensure we select tests that don't have any user specified tags (preserve old behavior)
      # - "aws" to ensure we select tests that now no longer have "default" because they have the "aws" tag
      # Ideally, refactor the tags themselves to be explicit about what cloud they are for and when they can run.
      # https://github.com/cockroachdb/cockroach/issues/100605
      FILTER="tag:aws tag:default"
    ;;
  aws)
    if [ -z "${FILTER}" ]; then
      FILTER="tag:aws"
    fi
    ;;
  *)
    echo "unknown cloud ${CLOUD}"
    exit 1
    ;;
esac
