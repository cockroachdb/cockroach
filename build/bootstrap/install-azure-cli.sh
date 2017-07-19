#!/usr/bin/env bash
#
# On a Debian/Ubuntu system, installs the Azure CLI.

set -euxo pipefail

# Instructions from https://docs.microsoft.com/en-us/cli/azure/install-azure-cli
echo "deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ wheezy main" | \
     sudo tee /etc/apt/sources.list.d/azure-cli.list

sudo apt-key adv --keyserver packages.microsoft.com --recv-keys 417A0893
sudo apt-get install -y apt-transport-https
sudo apt-get update
sudo apt-get install -y azure-cli
