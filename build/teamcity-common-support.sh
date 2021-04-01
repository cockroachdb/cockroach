# Common logic shared by build/teamcity-support.sh and build/release/teamcity-support.sh.

remove_files_on_exit() {
  rm -f .google-credentials.json
  rm -f .cockroach-teamcity-key
  rm -rf ~/.docker
  rm -f ~/.ssh/id_rsa{,.pub}
}
trap remove_files_on_exit EXIT

log_into_gcloud() {
  if [[ "${google_credentials}" ]]; then
    echo "${google_credentials}" > .google-credentials.json
    gcloud auth activate-service-account --key-file=.google-credentials.json
  else
    echo 'warning: `google_credentials` not set' >&2
  fi
}
