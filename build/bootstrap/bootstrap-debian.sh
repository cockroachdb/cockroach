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
  build-essential \
  cmake \
  docker.io \
  libtinfo-dev \
  git \
  nodejs \
  yarn

curl -fsSL https://storage.googleapis.com/golang/go1.9.2.linux-amd64.tar.gz | sudo tar -C /usr/local -xz
sudo ln -s /usr/local/go/bin/* /usr/local/bin

sudo adduser "${USER}" docker

go get -d github.com/cockroachdb/cockroach
