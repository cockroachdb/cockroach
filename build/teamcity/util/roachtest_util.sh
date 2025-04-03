# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Common logic used by the nightly roachtest scripts (Bazel and non-Bazel).

# Set up Google credentials. Note that we need this for all clouds since we upload
# perf artifacts to Google Storage at the end.
if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity

  # Set GOOGLE_APPLICATION_CREDENTIALS so that gcp go libraries can find it.
  export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/creds.json"
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

# defines get_host_arch
source  $root/build/teamcity/util/roachtest_arch_util.sh

# Early bind the stats dir. Roachtest invocations can take ages, and we want the
# date at the time of the start of the run (which identifies the version of the
# code run best).
stats_dir="$(date +"%Y%m%d")-${TC_BUILD_ID}"
stats_file_name="stats.json"

# Provide a default value for EXPORT_OPENMETRICS if it is not set
EXPORT_OPENMETRICS="${EXPORT_OPENMETRICS:-false}"

if [[ "${EXPORT_OPENMETRICS}" == "true" ]]; then
  stats_file_name="stats.om"
fi

# Set up a function we'll invoke at the end.
function upload_stats {
  if tc_release_branch; then
    bucket="${ROACHTEST_BUCKET:-cockroach-nightly-${CLOUD}}"
    if [[ "${EXPORT_OPENMETRICS}" == "true" ]]; then

        # TODO(sambhav-jain-16): Change the bucket after new buckets are created
        bucket="${ROACHTEST_BUCKET:-cockroach-testeng-metrics/omloader/incoming/${CLOUD}}"
    fi

    if [[ "${CLOUD}" == "gce" && "${EXPORT_OPENMETRICS}" == "false" ]]; then
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
    # TODO: FIPS_ENABLED is deprecated, use roachtest --metamorphic-fips-probability, instead.
    # In FIPS-mode, keep artifacts separate by using the 'fips' suffix.
    if [[ ${FIPS_ENABLED:-0} == 1 ]]; then
      remote_artifacts_dir="${remote_artifacts_dir}-fips"
    fi

    # If using openmetrics, activate new service account for uploading to openmetrics bucket
    if [[ "${EXPORT_OPENMETRICS}" == "true" && "$ROACHPERF_OPENMETRICS_CREDENTIALS" ]]; then
      echo "$ROACHPERF_OPENMETRICS_CREDENTIALS" > roachperf.json
      gcloud auth activate-service-account --key-file=roachperf.json
    fi

    # The ${stats_file_name} files need some path translation:
    #     ${artifacts}/path/to/test/${stats_file_name}
    # to
    #     gs://${bucket}/artifacts/${stats_dir}/path/to/test/${stats_file_name}
    #
    # `find` below will expand "{}" as ./path/to/test/${stats_file_name}. We need
    # to bend over backwards to remove the `./` prefix or gsutil will have
    # a `.` folder in ${stats_dir}, which we don't want.
    (cd "${artifacts}" && \
      while IFS= read -r f; do
        if [[ -n "${f}" ]]; then
          artifacts_dir="${remote_artifacts_dir}"
          # If 'cpu_arch=xxx' is encoded in the path, use it as suffix to separate artifacts by cpu_arch.
          if [[ "${f}" == *"/cpu_arch=arm64/"* ]]; then
            artifacts_dir="${artifacts_dir}-arm64"
          elif [[ "${f}" == *"/cpu_arch=fips/"* ]]; then
            artifacts_dir="${artifacts_dir}-fips"
          fi
          gsutil cp "${f}" "gs://${bucket}/${artifacts_dir}/${stats_dir}/${f}"
        fi
      done <<< "$(find . -name ${stats_file_name} | sed 's/^\.\///')")
  fi
}

set -x

# Uploads roachprod and roachtest binaries to GCS.
function upload_binaries {
  if tc_release_branch; then
      bucket="cockroach-nightly"
      branch=$(tc_build_branch)
      arch=$(get_host_arch)
      os=linux
      sha=${BUILD_VCS_NUMBER}
      gsutil cp bin/roachprod gs://$bucket/binaries/$branch/$os/$arch/roachprod.$sha
      gsutil cp bin/roachtest gs://$bucket/binaries/$branch/$os/$arch/roachtest.$sha
      # N.B. both binaries are built from the same SHA, so one blob will suffice.
      echo "$sha" | gsutil cp - gs://$bucket/binaries/$branch/$os/$arch/latest_sha
  fi
}

function upload_all {
  upload_binaries
  upload_stats
}

# Upload any ${stats_file_name} we can find, and some binaries, no matter what happens.
trap upload_all EXIT

# Set up the parameters for the roachtest invocation.
PARALLELISM="${PARALLELISM-16}"
CPUQUOTA="${CPUQUOTA-1024}"
TESTS="${TESTS-}"
