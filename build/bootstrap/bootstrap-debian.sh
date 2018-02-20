#!/usr/bin/env bash
#
# On a Debian/Ubuntu system, bootstraps a docker install and the cockroach
# repo.

set -euxo pipefail

curl -fsSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | sudo apt-key add -
echo "deb https://deb.nodesource.com/node_6.x xenial main" | sudo tee /etc/apt/sources.list.d/nodesource.list

curl -fsSL https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add -
echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list

sudo apt-get update
sudo apt-get dist-upgrade -y
sudo apt-get install -y --no-install-recommends \
  autoconf \
  cmake \
  ccache \
  docker.io \
  libtinfo-dev \
  git \
  nodejs \
  yarn

sudo adduser "${USER}" docker

# Configure environment variables
echo 'export PATH="/usr/lib/ccache:${PATH}"' >> ~/.bashrc_bootstrap
# NB: GOPATH defaults to ${HOME}/go (but maybe having it set for the remainder
# of the script is enough reason to keep it here).
echo 'export GOPATH=${HOME}/go' >> ~/.bashrc_bootstrap
echo '. ~/.bashrc_bootstrap' >> ~/.bashrc

. ~/.bashrc_bootstrap

mkdir -p "$GOPATH/src/github.com/cockroachdb"

git clone https://github.com/cockroachdb/cockroach.git "$GOPATH/src/github.com/cockroachdb/cockroach"

. bootstrap/bootstrap-go.sh
. bootstrap/bootstrap-unison.sh
