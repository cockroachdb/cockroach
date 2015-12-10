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

BUCKET_NAME="cockroach"
LATEST_SUFFIX=".LATEST"
REPO_NAME="cockroach"
SHA="${CIRCLE_SHA1-$(git rev-parse HEAD)}"

# push_one_binary takes the path to the binary inside the repo.
# eg: push_one_binary sql/sql.test
# The file will be pushed to: s3://BUCKET_NAME/REPO_NAME/sql.test.SHA
# The S3 basename will be stored in s3://BUCKET_NAME/REPO_NAME/sql.test.LATEST
function push_one_binary {
  rel_path=$1
  binary_name=$(basename $1)

  cd $(dirname $0)/..
  time aws s3 cp ${rel_path} s3://${BUCKET_NAME}/${REPO_NAME}/${binary_name}.${SHA}

  # Upload LATEST file.
  tmpfile=$(mktemp /tmp/cockroach-push.XXXXXX)
  echo ${SHA} > ${tmpfile}
  time aws s3 cp ${tmpfile} s3://${BUCKET_NAME}/${REPO_NAME}/${binary_name}${LATEST_SUFFIX}
  rm -f ${tmpfile}
}

push_one_binary cockroach
push_one_binary sql/sql.test
push_one_binary acceptance/acceptance.test
