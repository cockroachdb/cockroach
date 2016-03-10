#!/usr/bin/env bash
# This file uses `bash` and not `sh` due to the `time` builtin (the external
# `time` is not available on CircleCI).

set -eu

BUCKET_NAME="binaries.cockroachdb.com"

# $0 takes the path to the binary inside the repo and the tarball base name.
# eg: $0 cockroach-darwin-10.9-amd64 cockroach.darwin-amd64
#   generates cockroach.darwin-amd64.tgz which expands into cockroach.darwin-amd64/cockroach
#   copies tarball to s3://BUCKET_NAME/cockroach.darwin-amd64.tgz

rel_path=$1
tarball_base=$2

cd "$(dirname "${0}")/.."

tmpdir=$(mktemp -d /tmp/cockroach-push.XXXXXX)
mkdir -p "${tmpdir}/${tarball_base}"

# Make sure the binary is named 'cockroach'.
cp ${rel_path} "${tmpdir}/${tarball_base}/cockroach"
time tar cz -C ${tmpdir} -f ${tarball_base}.tgz ${tarball_base}
time aws s3 cp ${tarball_base}.tgz s3://${BUCKET_NAME}/${tarball_base}.tgz

rm -rf ${tmpdir}
