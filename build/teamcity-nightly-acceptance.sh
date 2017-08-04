#!/usr/bin/env bash

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

TESTNAME=$1

case $TESTNAME in
  TestUpreplicate_1To3Small)
    TESTTIMEOUT=2h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608'
    ;;
  TestRebalance_3To5Small)
    TESTTIMEOUT=2h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608 -at.std-dev 14'
    ;;
  TestRebalance_3To5Small_WithSchemaChanges)
    TESTTIMEOUT=24h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608 -at.std-dev 14'
    ;;
  TestSteady_3Small)
    TESTTIMEOUT=24h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608 -at.std-dev 14'
    ;;
  TestSteady_6Medium)
    TESTTIMEOUT=24h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608 -at.std-dev 100'
    ;;
  TestUpreplicate_1To6Medium)
    TESTTIMEOUT=18h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608 -at.std-dev 52'
    ;;
  TestContinuousLoad_BlockWriter)
    TESTTIMEOUT=6h
    COCKROACH_EXTRA_FLAGS+=' -nodes 4'
    ;;
  TestContinuousLoad_Photos)
    TESTTIMEOUT=6h
    COCKROACH_EXTRA_FLAGS+=' -nodes 4'
    ;;
  *)
    echo "unknown test name $TESTNAME"
    exit 1
    ;;
esac

build/builder.sh sh -c '[ -f ~/.ssh/terraform ] || ssh-keygen -f ~/.ssh/terraform -N '\'\'

# The log files that should be created by -l below can only
# be created if the parent directory already exists. Ensure
# that it exists before running the test.
mkdir -p artifacts/acceptance
export TMPDIR=$PWD/artifacts/acceptance

build/builder.sh \
  env ARM_SUBSCRIPTION_ID="$ARM_SUBSCRIPTION_ID" ARM_CLIENT_ID="$ARM_CLIENT_ID" ARM_CLIENT_SECRET="$ARM_CLIENT_SECRET" ARM_TENANT_ID="$ARM_TENANT_ID" \
  make test \
  TESTS="\A$TESTNAME\z" \
  TESTTIMEOUT=$TESTTIMEOUT \
  PKG=./pkg/acceptance \
  TESTFLAGS="-v -show-logs --remote -key-name terraform -l $TMPDIR $COCKROACH_EXTRA_FLAGS" \
  | go-test-teamcity
