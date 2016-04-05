#!/bin/bash
# This script takes the name of a binary and downloads it from S3.
# It takes the repo_name/binary_name and an optional sha.
# If the sha is not specified, the latest binary is downloaded.
# to download latest binary:
# ./download_binary.sh cockroach/sql.test
# to download a specific sha:
# ./download_binary.sh examples-go/block_writer f1ab4265aa72cda950fdd032de3f2dbecaeff866
#
# This downloads the binary to the local directory with name: <binary>.<sha>
# and creates a symlink: <binary> -> <binary>.<sha>
#
# This is meant to be copied over to the instance first, then invoked
# in a "remote-exec" provisioner.
#
# eg:
# myexample.tf <<<
# ...
# provisioner "file" {
#   source = "download_binary.sh"
#   destination = "/home/ubuntu/download_binary.sh"
# }
# provisioner "remote-exec" {
#   inline = [
#     "bash download_binary.sh examples-go/block_writer",
#     "nohup ./block_writer --db-url=localhost:26259 > example.STDOUT 2>&1 &",
#     "sleep 5",
#   ]
# }
# ...
# <<<
set -ex

BUCKET_NAME="cockroach"
LATEST_SUFFIX=".LATEST"

binary_path=$1
if [ -z "${binary_path}" ]; then
  echo "binary not specified, run with: $0 [repo-name]/[binary-name]"
  exit 1
fi

sha=$2
if [ -z "${sha}" ]; then
  echo "Looking for latest sha for ${binary_path}"
  latest_url="https://s3.amazonaws.com/${BUCKET_NAME}/${binary_path}${LATEST_SUFFIX}"
  sha=$(curl ${latest_url})
  if [ -z "${sha}" ]; then
    echo "Could not fetch latest binary: ${latest_url}"
    exit 1
  fi
fi

# Fetch binary and symlink.
binary_url="https://s3.amazonaws.com/${BUCKET_NAME}/${binary_path}.${sha}"
time curl -O ${binary_url}

# Chmod and symlink.
binary_name=$(basename ${binary_path})
chmod 755 ${binary_name}.${sha}
ln -s -f ${binary_name}.${sha} ${binary_name}

echo "Successfully fetched ${binary_path}.${sha}"
