#!/usr/bin/env bash
# This file uses `bash` and not `sh` due to the `time` builtin (the external
# `time` is not available on CircleCI).

set -eu

BUCKET_NAME="cockroach"
LATEST_SUFFIX=".LATEST"
REPO_NAME="cockroach"

# $0 takes the path to the binary inside the repo.
# eg: $0 sql/sql.test sql/sql-foo.test
# The file will be pushed to: s3://BUCKET_NAME/REPO_NAME/sql-foo.test.SHA
# The binary's sha will be stored in s3://BUCKET_NAME/REPO_NAME/sql-foo.test.LATEST
# The .LATEST file will also redirect to the latest binary when fetching through
# the S3 static-website.

sha=$1
rel_path=$2
binary_name=${3-$(basename "${2}")}

cd "$(dirname "${0}")/.."
time aws s3 cp ${rel_path} s3://${BUCKET_NAME}/${REPO_NAME}/${binary_name}.${sha}

# Upload LATEST file.
tmpfile=$(mktemp /tmp/cockroach-push.XXXXXX)
echo ${sha} > ${tmpfile}
time aws s3 cp --website-redirect "/${REPO_NAME}/${binary_name}.${sha}" ${tmpfile} s3://${BUCKET_NAME}/${REPO_NAME}/${binary_name}${LATEST_SUFFIX}
rm -f ${tmpfile}
