#!/usr/bin/env bash
#
# On a Debian/Ubuntu system, bootstraps a docker install and the cockroach
# repo.

set -euxo pipefail

sudo apt-get update
sudo apt-get dist-upgrade -y
sudo apt-get install -y --no-install-recommends docker.io git golang

sudo adduser "${USER}" docker

# Configure environment variables
echo 'export GOPATH=${HOME}/go' >> ~/.bashrc_go
echo 'export PATH=${GOPATH}/bin:${PATH}' >> ~/.bashrc_go
echo '. ~/.bashrc_go' >> ~/.bashrc

. ~/.bashrc_go

go get -d github.com/cockroachdb/cockroach
