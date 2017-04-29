#!/usr/bin/env bash
#
# On a Debian/Ubuntu system, bootstraps a docker install and the cockroach
# repo.

set -euxo pipefail

sudo apt-get update
sudo apt-get dist-upgrade -y
sudo apt-get install -y --no-install-recommends docker.io git cmake

sudo adduser "${USER}" docker

# Configure environment variables
echo 'export GOPATH=${HOME}/go' >> ~/.bashrc_go
echo '. ~/.bashrc_go' >> ~/.bashrc

. ~/.bashrc_go

mkdir -p "$GOPATH/src/github.com/cockroachdb"

git clone https://github.com/cockroachdb/cockroach.git "$GOPATH/src/github.com/cockroachdb/cockroach"

. bootstrap/bootstrap-go.sh
