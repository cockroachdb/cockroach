#!/usr/bin/env bash

set -euxo pipefail

# This script takes the name of a binary and downloads it from S3.
#
# It takes the repo_name/binary_name, an optional sha, and an optional name
# for the symlink it creates.
#
# If the sha is not specified, the latest binary is downloaded.
# to download latest binary:
# ./download_binary.sh cockroach/sql.test
# to download a specific sha:
# ./download_binary.sh examples-go/block_writer f1ab4265aa72cda950fdd032de3f2dbecaeff866
#
# This downloads the binary to the local directory with name: <binary>.<sha>
# and creates a symlink: <binary> -> <binary>.<sha> (unless a different name
# is specified).
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

URL_BASE=https://edge-binaries.cockroachdb.com

binary_path=${1-}
if [ -z "${binary_path}" ]; then
  echo "binary not specified, run with: $0 [repo-name]/[binary-name]"
  exit 1
fi

sha=${2-}
if [ -z "${sha}" ]; then
  binary_url=$(curl -sfSL -I -o /dev/null -w '%{url_effective}' ${URL_BASE}/"${binary_path}".LATEST)
else
  binary_url=${URL_BASE}/${binary_path}.${sha}
fi
echo "Downloading ${binary_url}"

linkname=${3-$(basename "${binary_path}")}

# Fetch binary, chmod, and symlink.
binary_name=$(curl -sfSL -O "${binary_url}" -w '%{filename_effective}')
chmod 755 "${binary_name}"
ln -s -f "${binary_name}" "$linkname"

echo "Successfully fetched ${binary_name}"
