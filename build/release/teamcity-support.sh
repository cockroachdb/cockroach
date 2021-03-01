# Common helpers for teamcity-*.sh scripts.

remove_files_on_exit() {
  rm -f .google-credentials.json
  rm -f .cockroach-teamcity-key
  rm -rf ~/.docker
}
trap remove_files_on_exit EXIT

tc_start_block() {
  echo "##teamcity[blockOpened name='$1']"
}

tc_end_block() {
  echo "##teamcity[blockClosed name='$1']"
}

log_into_gcloud() {
  if [[ "${google_credentials}" ]]; then
    echo "${google_credentials}" > .google-credentials.json
    gcloud auth activate-service-account --key-file=.google-credentials.json
  else
    echo 'warning: `google_credentials` not set' >&2
  fi
}

docker_login_with_google() {
  # https://cloud.google.com/container-registry/docs/advanced-authentication#json-key
  echo "${google_credentials}" | docker login -u _json_key --password-stdin "https://${gcr_hostname}"
}

docker_login() {
  echo "${DOCKER_ACCESS_TOKEN}" | docker login --username "${DOCKER_ID}" --password-stdin
}

configure_docker_creds() {
  # Work around headless d-bus problem by forcing docker to use
  # plain-text credentials for dockerhub.
  # https://github.com/docker/docker-credential-helpers/issues/105#issuecomment-420480401
  mkdir -p ~/.docker
  cat << EOF > ~/.docker/config.json
{
  "credsStore" : "",
  "auths": {
    "https://index.docker.io/v1/" : {
    }
  }
}
EOF
}

configure_git_ssh_key() {
  # Write a private key file and populate known_hosts
  touch .cockroach-teamcity-key
  chmod 600 .cockroach-teamcity-key
  echo "${github_ssh_key}" > .cockroach-teamcity-key

  mkdir -p "$HOME/.ssh"
  ssh-keyscan github.com > "$HOME/.ssh/known_hosts"
}

push_to_git() {
  # $@ passes all arguments to this function to the command
  GIT_SSH_COMMAND="ssh -i .cockroach-teamcity-key" git push "$@"
}
