#!/usr/bin/env bash
set -exuo pipefail

# This is intended to run in a CI to ensure dependencies are properly vendored.

echo "installing vendored glide"
go install ./vendor/github.com/Masterminds/glide

echo "checking that 'vendor' matches manifest"
./scripts/glide.sh install
! git -C vendor status --porcelain | read || (git -C vendor status; git -C vendor diff -a 1>&2; exit 1)

echo "all 3rd-party dependencies appear to be properly vendored."
