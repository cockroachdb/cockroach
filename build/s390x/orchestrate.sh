#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# See the following for documentation on how to use this script:
#   https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/5312806925/Setting+up+an+s390x+machine+as+a+TeamCity+agent
# Note there is a PREREQUISITE to set up the block storage for the agent's
# home directory and to create the agent user. See these docs for information.

# This script sets up a fresh Ubuntu VM running on s390x in IBM Cloud such
# that it can act as a TeamCity agent.

# In order for this script to work, you will have to put all of the
# scripts in this source directory (build/s390x) onto the VM, then execute
# this script.

# Note this is set up specifically to work on an Ubuntu 24.04 machine.

set -euxo pipefail

sudo chage -I -1 -m 0 -M -1 -E -1 ubuntu
sudo passwd -l ubuntu

./buildbazel.sh

sudo cp /home/ubuntu/s390x/bazel/output/bazel /usr/local/bin
sudo chmod a+rx /usr/local/bin/bazel

./teamcity-agent.sh
