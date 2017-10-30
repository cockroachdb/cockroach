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
sudo apt-get install -y --no-install-recommends docker.io git autoconf cmake nodejs yarn

sudo adduser "${USER}" docker

# Configure environment variables
echo 'export GOPATH=${HOME}/go' >> ~/.bashrc_go
echo '. ~/.bashrc_go' >> ~/.bashrc

. ~/.bashrc_go

mkdir -p "$GOPATH/src/github.com/cockroachdb"

git clone https://github.com/cockroachdb/cockroach.git "$GOPATH/src/github.com/cockroachdb/cockroach"

. bootstrap/bootstrap-go.sh
