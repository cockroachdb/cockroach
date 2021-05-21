# Common logic shared by build/teamcity-support.sh and build/release/teamcity-support.sh.

# Call this to clean up after using any other functions from this file.
common_support_remove_files_on_exit() {
  rm -f .cockroach-teamcity-key
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

configure_git_ssh_key() {
  # Write a private key file and populate known_hosts
  touch .cockroach-teamcity-key
  chmod 600 .cockroach-teamcity-key
  echo "${github_ssh_key}" > .cockroach-teamcity-key

  mkdir -p "$HOME/.ssh"
  ssh-keyscan github.com > "$HOME/.ssh/known_hosts"
}

git_wrapped() {
  # $@ passes all arguments to this function to the command
  GIT_SSH_COMMAND="ssh -i .cockroach-teamcity-key" git "$@"
}
