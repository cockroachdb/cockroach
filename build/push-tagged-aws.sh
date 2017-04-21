#!/usr/bin/env bash
# Push tagged binaries to AWS.
# This is run by TeamCity when building due to a tag.
#
# Prerequisites:
# - binaries must be statically linked by running build/build-static-binaries.sh
# - TeamCity must have AWS credentials configured
# - AWS credentials must have S3 write permissions on the bucket
# - the AWS CLI must be installed on the machine
# - the region must be configured
#
# Ask marc@cockroachlabs.com for the AWS credentials file to use.

set -eux

cd "$(dirname "${0}")"/..

BUCKET_NAME=binaries.cockroachdb.com

deploy_file() {
  time aws s3 cp "$1" "s3://${BUCKET_NAME}/$1"
}

# make_and_deploy_archive takes the path to the binary inside the repo and the
# archive base name.
# eg: make_and_deploy_archive cockroach-darwin-10.9-amd64 cockroach.darwin-amd64
#   generates cockroach.darwin-amd64.tgz which expands into cockroach.darwin-amd64/cockroach
#   copies archive to s3://BUCKET_NAME/cockroach.darwin-amd64.tgz
make_and_deploy_archive() {
  rel_path=$1
  tarball_base=$2

  tmpdir=$(mktemp -d /tmp/cockroach-push.XXXXXX)
  mkdir -p "${tmpdir}/${tarball_base}"

  # Make sure the binary is named 'cockroach' (or 'cockroach.exe' for the
  # Windows binary).
  case "${rel_path}" in
  *.exe)
    cp "${rel_path}" "${tmpdir}/${tarball_base}/cockroach.exe"
    pushd "$tmpdir"
    time zip -r "${tarball_base}".zip "${tarball_base}"
    deploy_file "${tarball_base}.zip"
    popd
    ;;
  *)
    cp "${rel_path}" "${tmpdir}/${tarball_base}/cockroach"
    time tar cz -C "${tmpdir}" -f "${tarball_base}".tgz "${tarball_base}"
    deploy_file "${tarball_base}.tgz"
    ;;
  esac
  rm -rf "${tmpdir}"
}

# Tar up and push binaries. One tagged, one called "latest".
make_and_deploy_archive cockroach cockroach-"${VERSION}".linux-amd64
make_and_deploy_archive cockroach cockroach-latest.linux-amd64
make_and_deploy_archive cockroach-linux-2.6.32-musl-amd64 cockroach-"${VERSION}".linux-musl-amd64
make_and_deploy_archive cockroach-linux-2.6.32-musl-amd64 cockroach-latest.linux-musl-amd64
make_and_deploy_archive cockroach-darwin-10.9-amd64 cockroach-"${VERSION}".darwin-10.9-amd64
make_and_deploy_archive cockroach-darwin-10.9-amd64 cockroach-latest.darwin-10.9-amd64
make_and_deploy_archive cockroach-windows-6.2-amd64.exe cockroach-"${VERSION}".windows-6.2-amd64
make_and_deploy_archive cockroach-windows-6.2-amd64.exe cockroach-latest.windows-6.2-amd64

# Push source tarball for Homebrew and other package managers that like
# tarballs. One tagged, one called "latest".
deploy_file cockroach-latest.src.tgz
deploy_file cockroach-"${VERSION}".src.tgz
