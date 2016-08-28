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

SHA="${CIRCLE_SHA1-$(git rev-parse HEAD)}"

# Linux binaries: cockroach + tests.
./push-one-binary.sh ${SHA} cockroach cockroach
./push-one-binary.sh ${SHA} sql/sql.test
./push-one-binary.sh ${SHA} acceptance/acceptance.test
./push-one-binary.sh ${SHA} static-tests.tar.gz

# TODO(marc): use these instead of the above "Linux binaries", this requires
# fixing the callers.
./push-one-binary.sh ${SHA} cockroach cockroach.linux-amd64
./push-one-binary.sh ${SHA} cockroach-darwin-10.9-amd64 cockroach.darwin-amd64
