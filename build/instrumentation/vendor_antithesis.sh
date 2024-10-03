#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -eo pipefail

# NB: Currently, this script is only intended to work with `make instrument(short)`
# rather than Bazel, as it modifies the instrumented code copy outside the sandbox.

function fail {
  echo -e "Error: $1"
  exit 1
}

ANTITHESIS_TMP=$1

if [ -z "$ANTITHESIS_TMP" ]; then
    fail "must call script with instrumentation output directory"
fi

# Symlink the Antithesis library, including wrapper and generated module, into vendor.
rm -rf ${ANTITHESIS_TMP}/customer/vendor/antithesis.com
mkdir -p ${ANTITHESIS_TMP}/customer/vendor/antithesis.com/go
ln -s ${ANTITHESIS_TMP}/antithesis ${ANTITHESIS_TMP}/customer/vendor/antithesis.com/go/instrumentation

# Add Antithesis to vendor/modules.txt explicitly (to avoid rerunning `go mod vendor`).
cp vendor/modules.txt ${ANTITHESIS_TMP}/customer/vendor/modules.txt
echo "# antithesis.com/go/instrumentation v1.0.0 => ${ANTITHESIS_TMP}/antithesis" > ${ANTITHESIS_TMP}/customer/vendor/modules.tmp
echo "## explicit; go 1.14" >> ${ANTITHESIS_TMP}/customer/vendor/modules.tmp
echo "antithesis.com/go/instrumentation" >> ${ANTITHESIS_TMP}/customer/vendor/modules.tmp
(cd ${ANTITHESIS_TMP}/customer/vendor && find -L antithesis.com -type d -name "instrumented_module*" >> ${ANTITHESIS_TMP}/customer/vendor/modules.tmp)
cat ${ANTITHESIS_TMP}/customer/vendor/modules.tmp vendor/modules.txt > ${ANTITHESIS_TMP}/customer/vendor/modules.txt
echo "# antithesis.com/go/instrumentation => ${ANTITHESIS_TMP}/antithesis" >> ${ANTITHESIS_TMP}/customer/vendor/modules.txt
rm ${ANTITHESIS_TMP}/customer/vendor/modules.tmp
