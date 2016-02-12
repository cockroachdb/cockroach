#!/bin/bash
# Push binaries to AWS.
# This is run by circle-ci after successful docker push.
#
# Requisites:
# - binaries must be statically linked by running build/build-static-binaries.sh
# - circleci must have AWS credentials configured
# - AWS credentials must have S3 write permissions on the bucket
# - the aws cli must be installed on the machine
# - the region must be configured
#
# Ask marc@cockroachlabs.com for the aws credentials file to use, then follow the
# steps in circle.yml to configure aws and generate the binaries.

set -eux

source $(dirname $0)/build-common.sh

OSARCH="linux-amd64"
SHA="${CIRCLE_SHA1-$(git rev-parse HEAD)}"

push_one_binary ${SHA} cockroach cockroach-${OSARCH}
push_one_binary ${SHA} sql/sql-${OSARCH}.test
push_one_binary ${SHA} acceptance/acceptance-${OSARCH}.test
push_one_binary ${SHA} static-tests-${OSARCH}.tar.gz
