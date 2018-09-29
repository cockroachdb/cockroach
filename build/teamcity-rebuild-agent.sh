#!/usr/bin/env bash

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

# NB: You're going to want to create a build/packer/credentials.json file.
docker run \
  --rm --interactive --tty \
  --env GOOGLE_APPLICATION_CREDENTIALS=/packer/credentials.json \
  --env=DIGITALOCEAN_API_TOKEN \
  --volume="$PWD/build/packer:/packer" \
  --workdir=/packer \
  hashicorp/packer:1.1.0 \
  build --on-error=ask teamcity-agent.json
