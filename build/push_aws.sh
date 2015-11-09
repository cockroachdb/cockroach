#!/bin/bash
# Push cockroach binaries to AWS.
# This is run by circle-ci after successful docker push.
# Requisites:
# - circleci must have AWS credentials configured
# - AWS credentials must have S3 write permissions on the bucket
# - the aws cli must be installed on the machine
# - the region must be configured

set -eux

BUCKET_PATH="cockroachdb/bin"
BINARY_NAME="cockroach.$(date +"%Y%m%d-%H:%M:%S").${CIRCLE_SHA1}"

cd $(dirname $0)/..
strip -S cockroach
time aws s3 cp cockroach s3://${BUCKET_PATH}/${BINARY_NAME}
