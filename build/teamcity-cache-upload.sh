#!/usr/bin/env bash

set -euxo pipefail

BUCKET_NAME="cockroach"
REPO_NAME="cockroach"
TMPDIR=$(mktemp -d)

echo "Cleaning repo..."
rm -rf "${HOME}/.jspm/*"
rm -rf "${HOME}/.yarn-cache/*"
mv "${GOPATH}/src/github.com/cockroachdb/cockroach" "{$TMPDIR}/"
rm -rf "${GOPATH}"
mkdir -p "${GOPATH}/src/github.com/cockroachdb"
mv "${TMPDIR}/cockroach" "${GOPATH}/src/github.com/cockroachdb/"

echo "Building cache..."
cd "${GOPATH}/src/github.com/cockroachdb/cockroach"
build/builder.sh make gotestdashi
build/builder.sh make gotestdashi GOFLAGS=-race
build/builder.sh make .bootstrap
build/builder.sh go generate ./pkg/...

echo "Archiving cache..."
time tar -czf "${TMPDIR}/teamcity-cache.tgz" ~/work/.go ~/.jspm ~/.yarn-cache

echo "Uploading cache..."
time aws s3 cp "${TMPDIR}/teamcity-cache.tgz" "s3://${BUCKET_NAME}/${REPO_NAME}/teamcity-cache.tgz"

rm -rf "${TMPDIR}"
