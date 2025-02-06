#!/usr/bin/env bash

# Copyright 2017 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#
# On a Debian/Ubuntu system, bootstraps all the required dependencies for the
# cockroach & managed-service repos.

set -euxo pipefail

curl -fsSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | sudo apt-key add -
echo "deb https://deb.nodesource.com/node_16.x focal main" | sudo tee /etc/apt/sources.list.d/nodesource.list

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold"
sudo apt-get install -y --no-install-recommends \
  mosh \
  autoconf \
  docker.io \
  libncurses-dev \
  libresolv-wrapper \
  make \
  gcc \
  g++ \
  git \
  nodejs \
  bison

# pnpm doesn't provide a Debian repository, and supports either `curl | sh` or `npm install -g` installations.
curl -fsSL https://get.pnpm.io/install.sh | env PNPM_VERSION=8.6.6 sh -
echo >> ~/.bashrc

sudo adduser "${USER}" docker

# Upgrade cmake.
trap 'rm -f /tmp/cmake.tgz' EXIT
curl -fsSL https://github.com/Kitware/CMake/releases/download/v3.20.3/cmake-3.20.3-Linux-x86_64.tar.gz >/tmp/cmake.tgz
sha256sum -c - <<EOF
97bf730372f9900b2dfb9206fccbcf92f5c7f3b502148b832e77451aa0f9e0e6  /tmp/cmake.tgz
EOF
sudo tar -C /usr --strip-components=1 -zxf /tmp/cmake.tgz && rm /tmp/cmake.tgz

# Install Go.
trap 'rm -f /tmp/go.tgz' EXIT
curl -fsSL https://dl.google.com/go/go1.23.6.linux-amd64.tar.gz >/tmp/go.tgz
sha256sum -c - <<EOF
9379441ea310de000f33a4dc767bd966e72ab2826270e038e78b2c53c2e7802d  /tmp/go.tgz
EOF
sudo tar -C /usr/local -zxf /tmp/go.tgz && rm /tmp/go.tgz

# Install Docker compose.
sudo curl -L "https://github.com/docker/compose/releases/download/v2.32.4/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose --version

# Install NVM.
# Note: you still required to run `nvm install <version>` to install a specific version of Node.js.
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
echo >> ~/.bashrc

# Install Bazelisk as Bazel.
# NOTE: you should keep this in sync with build/packer/teamcity-agent.sh and build/bazelbuilder/Dockerfile -- if
# an update is necessary here, it's probably necessary in the agent as well.
# Note: `dev` will refuse working if `ccache` is installed. Run `sudo apt remove ccache` to fix the issue.
curl -fsSL https://github.com/bazelbuild/bazelisk/releases/download/v1.10.1/bazelisk-linux-amd64 > /tmp/bazelisk
echo '4cb534c52cdd47a6223d4596d530e7c9c785438ab3b0a49ff347e991c210b2cd /tmp/bazelisk' | sha256sum -c -
chmod +x /tmp/bazelisk
sudo mv /tmp/bazelisk /usr/bin/bazel

# Install the Unison file-syncer.
. bootstrap/bootstrap-unison.sh

# Configure environment variables for CockroachDB
echo 'export PATH="${PATH}:${HOME}/go/src/github.com/cockroachdb/cockroach/bin:/usr/local/go/bin"' >> ~/.bashrc_bootstrap
echo >> ~/.bashrc
echo '. ~/.bashrc_bootstrap' >> ~/.bashrc
. ~/.bashrc_bootstrap

git clone https://github.com/cockroachdb/cockroach "$(go env GOPATH)/src/github.com/cockroachdb/cockroach"
git -C "$(go env GOPATH)/src/github.com/cockroachdb/cockroach" submodule update --init

