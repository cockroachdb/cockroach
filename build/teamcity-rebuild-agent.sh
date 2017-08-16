#!/usr/bin/env bash

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

docker run \
  --rm --interactive --tty \
  --env=DIGITALOCEAN_API_TOKEN \
  --volume="$PWD/build/packer:/packer" \
  --workdir=/packer \
  hashicorp/packer:1.1.0 \
  build teamcity-agent.json
