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
LATEST="LATEST"

now=$(date +"%Y%m%d-%H%M%S")
today=$(echo ${now} | cut -d '-' -f1)
binary_name="cockroach.${now}.${CIRCLE_SHA1}"

# Fetch name of most recent binary.
# TODO(marc): some versions of `aws s3 cp` allow - for stdin/stdout, but not all.
tmpfile=$(mktemp /tmp/cockroach-push.XXXXXX)
# Don't exit on errors, clearing the latest file is an easy way to force multiple uploads.
time aws s3 cp s3://${BUCKET_PATH}/${LATEST} ${tmpfile} || true
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
strip -S cockroach
time aws s3 cp cockroach s3://${BUCKET_PATH}/${binary_name}

# Upload LATEST file.
tmpfile=$(mktemp /tmp/cockroach-push.XXXXXX)
echo ${binary_name} > ${tmpfile}
time aws s3 cp ${tmpfile} s3://${BUCKET_PATH}/${LATEST}
rm -f ${tmpfile}
