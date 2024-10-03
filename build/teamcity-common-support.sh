# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

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

log_into_aws() {
  if [[ "${aws_access_key_id}" && "${aws_secret_access_key}" && "${aws_default_region}" ]]; then
    aws configure set aws_access_key_id "${aws_access_key_id}";
    aws configure set aws_secret_access_key "${aws_secret_access_key}";
    aws configure set default.region "${aws_default_region}";
  else
    echo 'warning: `aws_access_key_id` or `aws_secret_access_key` or `aws_default_region` not set' >&2
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
