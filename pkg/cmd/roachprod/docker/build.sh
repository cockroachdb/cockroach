#!/bin/bash
# This script is used to build the docker image.

set -e
set -o pipefail

# Install AWS, Azure, GCP SDKs per
# https://cloud.google.com/sdk/docs/quickstart-debian-ubuntu
# https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-apt?view=azure-cli-latest
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" |
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

curl https://packages.cloud.google.com/apt/doc/apt-key.gpg |
    apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add -

# Azure
apt-get update -y
apt-get install -y lsb-release

curl -sL https://packages.microsoft.com/keys/microsoft.asc |
    apt-key --keyring /usr/share/keyrings/microsoft.gpg  add -

AZ_REPO=$(lsb_release -cs)
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ $AZ_REPO main" |
    tee /etc/apt/sources.list.d/azure-cli.list

# Install packages and clean up
apt-get update -y
apt-get install google-cloud-sdk awscli azure-cli -y
rm -rf /var/lib/apt/lists/*

go get github.com/cockroachdb/cockroach/pkg/cmd/roachprod
