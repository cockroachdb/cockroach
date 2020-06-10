# Common helpers for teamcity-*.sh scripts.

# root is the absolute path to the root directory of the repository.
root=$(cd "$(dirname "$0")/.." && pwd)

run() {
  echo "$@"
  "$@"
}

tc_start_block() {
  echo "##teamcity[blockOpened name='$1']"
}

tc_end_block() {
  echo "##teamcity[blockClosed name='$1']"
}

log_into_gcloud() {
  # Set up Google credentials.
  if [[ "${GOOGLE_EPHEMERAL_CREDENTIALS}" ]]; then
    echo "${GOOGLE_EPHEMERAL_CREDENTIALS}" > creds.json
    gcloud auth activate-service-account --key-file=creds.json
  else
    echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  fi
}

log_into_dockerhub() {
  echo "${DOCKER_AUTH}" | docker login --username "${DOCKER_ID}" --password-stdin
}

configure_docker_creds() {
  # Configures creds for dockerhub and gcr
  cat << EOF > ~/.docker/config.json
{
  "credsStore" : "",
  "auths": {
    "https://index.docker.io/v1/" : {
    }
  },
  "credHelpers": {
    "gcr.io": "gcloud"
  }
}
EOF
}
