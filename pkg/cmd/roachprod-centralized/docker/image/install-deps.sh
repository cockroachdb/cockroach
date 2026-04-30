#!/bin/bash

# Copyright 2020 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script is used to build the docker image.

set -e
set -o pipefail

apt update -y
apt install -y curl gnupg apt-transport-https ca-certificates lsb-release unzip jq

# Install OpenTofu
curl -fsSL -o /usr/share/keyrings/opentofu.gpg https://get.opentofu.org/opentofu.gpg
curl -fsSL https://packages.opentofu.org/opentofu/tofu/gpgkey | gpg --dearmor -o /usr/share/keyrings/opentofu-repo.gpg >/dev/null
echo \
  "deb [signed-by=/usr/share/keyrings/opentofu.gpg,/usr/share/keyrings/opentofu-repo.gpg] https://packages.opentofu.org/opentofu/tofu/any/ any main
deb-src [signed-by=/usr/share/keyrings/opentofu.gpg,/usr/share/keyrings/opentofu-repo.gpg] https://packages.opentofu.org/opentofu/tofu/any/ any main" | \
  tee /etc/apt/sources.list.d/opentofu.list

apt update -y
apt install -y tofu

# Install Azure cli
# https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-apt?view=azure-cli-latest
curl -sL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg

AZ_REPO=$(lsb_release -cs)
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ $AZ_REPO main" |
    tee /etc/apt/sources.list.d/azure-cli.list

apt update -y
apt install -y azure-cli

# Debian ships with awscli version 1.x, which is unsupported by roachprod.
# Install aws-cli using the official instructions from
# https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-linux.html
# Detect architecture
ARCH=$(dpkg --print-architecture)
if [ "$ARCH" = "amd64" ]; then
    AWS_ARCH="x86_64"
    AWS_SHA="7ee475f22c1b35cc9e53affbf96a9ffce91706e154a9441d0d39cbf8366b718e"
elif [ "$ARCH" = "arm64" ]; then
    AWS_ARCH="aarch64"
    AWS_SHA="624ebb04705d4909eb0d56d467fe6b8b5c53a8c59375ed520e70236120125077"
else
    echo "Unsupported architecture: $ARCH"
    exit 1
fi

curl "https://awscli.amazonaws.com/awscli-exe-linux-${AWS_ARCH}-2.0.30.zip" -o "awscliv2.zip"
sha256sum -c - <<EOF
${AWS_SHA}  awscliv2.zip
EOF
unzip awscliv2.zip
./aws/install

# Cleanup
rm -rf aws awscliv2.zip
rm -rf /var/lib/apt/lists/*