#!/usr/bin/env bash

set -euo pipefail

BETA_TAG=${1-}

if [ -z ${BETA_TAG} ]; then
    echo "usage: $0 <beta-tag>"
    exit 1
fi

darwin=".darwin-10.9-amd64"
linux=".linux-amd64"

function check_aws() {
  echo -n "aws binaries"

  local aws_bins=$(aws s3 ls s3://binaries.cockroachdb.com |
                   grep ${BETA_TAG} | awk '{print $NF}')
  for suffix in ${darwin}.tgz ${linux}.tgz; do
    if ! echo ${aws_bins} | grep -q ${suffix}; then
      echo ": cockroach-${BETA_TAG}${suffix} not found"
      exit 1
    fi
  done

  echo ": ok"
}

function verify_tag() {
  if [ "$1" != "${BETA_TAG}" ]; then
    echo ": expected ${BETA_TAG}, but found $1"
    exit 1
  fi
}

function check_linux() {
  echo -n "linux binary"

  curl -s https://binaries.cockroachdb.com/cockroach-${BETA_TAG}${linux}.tgz | tar xz
  local tag=$($(dirname $0)/../build/builder.sh ./cockroach-${BETA_TAG}${linux}/cockroach version |
              grep 'Build Tag:' | awk '{print $NF}' | tr -d '\r')
  if test -z "$tag"; then
      # From v21.1 onwards.
      tag=$($(dirname $0)/../build/builder.sh ./cockroach-${BETA_TAG}${linux}/cockroach version --build-tag | tr -d '\r')
  fi
  rm -fr ./cockroach-${BETA_TAG}${linux}
  verify_tag "${tag}"

  echo ": ok"
}

function check_darwin() {
  echo -n "darwin binary"

  if [ "$(uname)" != "Darwin" ]; then
    echo ": not checked"
    return
  fi

  curl -s https://binaries.cockroachdb.com/cockroach-${BETA_TAG}${darwin}.tgz | tar xz
  local tag=$(./cockroach-${BETA_TAG}${darwin}/cockroach version |
		  grep 'Build Tag:' | awk '{print $NF}' | tr -d '\r')
  if test -z "$tag"; then
      # From v21.1 onwards.
      tag=$(./cockroach-${BETA_TAG}${darwin}/cockroach version | tr -d '\r')
  fi
  rm -fr cockroach-${BETA_TAG}${darwin}
  verify_tag "${tag}"

  echo ": ok"
}

function check_docker() {
  echo -n "docker"

  trap "rm -f docker.stderr" 0
  local tag=$(docker run --rm cockroachdb/cockroach:${BETA_TAG} version 2>docker.stderr |
              grep 'Build Tag:' | awk '{print $NF}' | tr -d '\r')
  if test -z "$tag"; then
      # From v21.1 onwards.
      tag=$(docker run --rm cockroachdb/cockroach:${BETA_TAG} version --build-tag 2>docker.stderr | tr -d '\r')
  fi
  if [ -z "${tag}" ]; then
      echo
      cat docker.stderr
      exit 1
  fi
  verify_tag "${tag}"

  echo ": ok"
}

check_aws
check_linux
check_darwin
check_docker
