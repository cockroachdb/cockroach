#!/usr/bin/env bash

# Required setup:
# 1. Have an Azure account.
# 2. Set the ARM_SUBSCRIPTION_ID, ARM_CLIENT_ID, ARM_CLIENT_SECRET, and
#    ARM_TENANT_ID variables as documented here:
#  https://www.terraform.io/docs/providers/azurerm/#argument-reference
# 3. If running on your local machine, have a symlink to `cat` named
#    `go-test-teamcity` on your PATH.

# Example use:
#
# COCKROACH_EXTRA_FLAGS='-tf.keep-cluster=failed' build/teamcity-nightly-acceptance.sh TestUpreplicate_1To3Small

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

syntax() {
  echo "syntax: $0 test_name"
}

if [[ -z "${1-}" ]]; then
  echo "missing test name"
  syntax
  exit 1
fi

TESTNAME=$1

PKG=./pkg/acceptance

case $TESTNAME in
  TestUpreplicate_1To3Small)
    TESTTIMEOUT=2h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608 -tf.cockroach-flags="--vmodule=allocator=5,allocator_scorer=5,replicate_queue=5"'
    ;;
  TestRebalance_3To5Small)
    TESTTIMEOUT=2h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608 -tf.cockroach-flags="--vmodule=allocator=5,allocator_scorer=5,replicate_queue=5" -at.std-dev 14'
    ;;
  TestRebalance_3To5Small_WithSchemaChanges)
    TESTTIMEOUT=24h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608 -tf.cockroach-flags="--vmodule=allocator=5,allocator_scorer=5,replicate_queue=5" -at.std-dev 14'
    ;;
  TestSteady_3Small)
    TESTTIMEOUT=24h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608 -at.std-dev 14'
    ;;
  TestSteady_6Medium)
    TESTTIMEOUT=24h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608 -tf.cockroach-flags="--vmodule=allocator=5,allocator_scorer=5,replicate_queue=5" -at.std-dev 100'
    ;;
  TestUpreplicate_1To6Medium)
    TESTTIMEOUT=18h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608 -tf.cockroach-flags="--vmodule=allocator=5,allocator_scorer=5,replicate_queue=5" -at.std-dev 52'
    ;;
  TestContinuousLoad_BlockWriter)
    TESTTIMEOUT=6h
    COCKROACH_EXTRA_FLAGS+=' -nodes 4'
    ;;
  TestContinuousLoad_Photos)
    TESTTIMEOUT=6h
    COCKROACH_EXTRA_FLAGS+=' -nodes 4'
    ;;
  BenchmarkRestoreTPCH10/numNodes=3)
    PKG=./pkg/ccl/acceptanceccl
    TESTTIMEOUT=2h
    COCKROACH_EXTRA_FLAGS+=' -tf.cockroach-env=COCKROACH_PREEMPTIVE_SNAPSHOT_RATE=8388608'
    ;;
  *)
    echo "unknown test name $TESTNAME"
    syntax
    exit 1
    ;;
esac

cd "$(dirname "${0}")"/..

pkg/acceptance/prepare.sh

[ -f ~/.ssh/terraform ] || ssh-keygen -f ~/.ssh/terraform -N ''

# The log files that should be created by -l below can only
# be created if the parent directory already exists. Ensure
# that it exists before running the test.
mkdir -p artifacts/acceptance
export TMPDIR=$PWD/artifacts/acceptance

TYPE=release-$(go env GOOS)
case $TYPE in
  *-linux)
    TYPE+=-gnu
    ;;
esac

case $TESTNAME in
  Benchmark*)
    COCKROACH_EXTRA_FLAGS+=" -test.bench \A$TESTNAME\z -test.run -"
    ;;
  Test*)
    COCKROACH_EXTRA_FLAGS+=" -test.run   \A$TESTNAME\z"
    ;;
  *)
    echo "unknown test name $TESTNAME"
    exit 1
    ;;
esac

build/builder.sh make TYPE=$TYPE testbuild PKG=$PKG
cd $PKG
# shellcheck disable=SC2086
./${PKG##*/}.test -l "$TMPDIR" -test.v -test.timeout $TESTTIMEOUT -show-logs -remote -key-name terraform \
  $COCKROACH_EXTRA_FLAGS | go-test-teamcity
