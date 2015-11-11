#!/bin/bash
# Push binaries to AWS.
# This is run by circle-ci after successful docker push.
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

BUCKET_PATH="cockroachdb/bin"
LATEST_SUFFIX=".LATEST"

now=$(date +"%Y%m%d-%H%M%S")
today=$(echo ${now} | cut -d '-' -f1)
binary_suffix=".${now}.${CIRCLE_SHA1-$(git rev-parse HEAD)}"

# push_one_binary takes the path to the binary inside the cockroach repo.
# eg: push_one_binary sql/sql.test
# The file will be pushed to: s3://BUCKET_PATH/sql.test.YYMMDD-HHMMSS.CIRCLESHA1
# The S3 basename will be stored in s3://BUCKET_PATH/sql.test.LATEST
function push_one_binary {
  rel_path=$1
  binary_name=$(basename $1)

  # Fetch name of most recent binary.
  # TODO(marc): some versions of `aws s3 cp` allow - for stdin/stdout, but not all.
  tmpfile=$(mktemp /tmp/cockroach-push.XXXXXX)
  # Don't exit on errors, clearing the latest file is an easy way to force multiple uploads.
  time aws s3 cp s3://${BUCKET_PATH}/${binary_name}${LATEST_SUFFIX} ${tmpfile} || true
  contents=$(cat ${tmpfile})
  rm -f ${tmpfile}
  latest_date=$(echo ${contents} | sed 's/cockroach.\([0-9]\+\)-.*/\1/')

  if [ "${latest_date}" == "${today}" ]; then
    echo "Latest binary is from today, skipping: ${contents}"
    exit 0
  fi

  # Latest file did not exist, was empty, or pointed to an old binary.
  # Upload binary.
  cd $(dirname $0)/..
  time aws s3 cp ${rel_path} s3://${BUCKET_PATH}/${binary_name}${binary_suffix}

  # Upload LATEST file.
  tmpfile=$(mktemp /tmp/cockroach-push.XXXXXX)
  echo ${binary_name}${binary_suffix} > ${tmpfile}
  time aws s3 cp ${tmpfile} s3://${BUCKET_PATH}/${binary_name}${LATEST_SUFFIX}
  rm -f ${tmpfile}
}

push_one_binary cockroach
push_one_binary sql/sql.test
