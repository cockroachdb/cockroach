#!/bin/bash

# Copyright 2020 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script is used to build the docker image.

set -e
set -o pipefail

# Install AWS and GCP CLIs per
# https://cloud.google.com/sdk/docs/quickstart-debian-ubuntu
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" |
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

curl https://packages.cloud.google.com/apt/doc/apt-key.gpg |
    apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add -

# Install packages and clean up
apt-get update -y
apt-get install -y google-cloud-sdk unzip
rm -rf /var/lib/apt/lists/*

# Debian ships with awscli version 1.x, which is unsupported by roachprod.
# Install aws-cli using the official instructions from
# https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-linux.html
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.0.30.zip" -o "awscliv2.zip"
sha256sum -c - <<EOF
7ee475f22c1b35cc9e53affbf96a9ffce91706e154a9441d0d39cbf8366b718e  awscliv2.zip
EOF
unzip awscliv2.zip
./aws/install
rm -rf aws awscliv2.zip
