#!/usr/bin/env bash
set -euxo pipefail
COCKROACH_PATH="${GOPATH}/src/github.com/cockroachdb/cockroach"
source "${COCKROACH_PATH}/build/jepsen-common.sh"

tc Started SetupCluster

progress Preparing files
# Copy the terraform config locally. We keep it in artifacts as well
# as the terraform state file so that if when troubleshooting a
# failing test we can reuse exactly the same settings.
cp -a "${COCKROACH_PATH}"/build/jepsen/terraform/* .

progress Generating controller SSH keys
rm -f controller.id_rsa controller.id_rsa.pub
ssh-keygen -f controller.id_rsa -N ''

progress Spinning up the cluster
# A failure here is caught by the trap handler.
terraform init
terraform apply --input=false --var=key_name="${KEY_NAME}"

tc Finished SetupCluster
