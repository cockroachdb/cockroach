#!/usr/bin/env sh
# Push binaries to AWS.
# This is run by circle-ci after successful docker push.
#
# Prerequisites:
# - binaries must be statically linked by running build/build-static-binaries.sh
# - circleci must have AWS credentials configured
# - AWS credentials must have S3 write permissions on the bucket
# - the aws cli must be installed on the machine
# - the region must be configured
#
# Ask marc@cockroachlabs.com for the aws credentials file to use, then follow the
# steps in circle.yml to configure aws and generate the binaries.

set -eux

cd "$(dirname "${0}")"

OSARCH="" # TODO(mberhault): should be "-linux-amd64"
SHA="${CIRCLE_SHA1-$(git rev-parse HEAD)}"

./push-one-binary.sh ${SHA} cockroach cockroach${OSARCH}
./push-one-binary.sh ${SHA} sql/sql${OSARCH}.test
./push-one-binary.sh ${SHA} acceptance/acceptance${OSARCH}.test
./push-one-binary.sh ${SHA} static-tests${OSARCH}.tar.gz
