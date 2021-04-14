# Common logic shared by build/teamcity-support.sh and build/release/teamcity-support.sh.

# Call this to clean up after using any other functions from this file.
common_support_remove_files_on_exit() {
  rm -f .google-credentials.json
}

log_into_gcloud() {
  if [[ "${google_credentials}" ]]; then
    echo "${google_credentials}" > .google-credentials.json
    gcloud auth activate-service-account --key-file=.google-credentials.json
  else
    echo 'warning: `google_credentials` not set' >&2
  fi
}
