#!/usr/bin/env bash

set -euxo pipefail

# The caller should download the static-tests and extract them into the
# local directory.

TEST_FLAGS="-test.bench=.* -test.benchmem -test.run=NONE -test.count=10"
LOG_DIR="logs"

# takes the full path to the test binary.
function run_one_test {
  test_binary=$1
  name=$(basename ${test_binary})
  package=$(dirname ${test_binary})
  log_dir="${LOG_DIR}/${package}"
  mkdir -p "${log_dir}"
  echo "${name}: ${test_binary} ${log_dir}"
  ${test_binary} ${TEST_FLAGS} 2> ${log_dir}/${name}.stderr > ${log_dir}/${name}.stdout
}

for test in $(find cockroach/ -type f -name '*.test*' | sort); do
  run_one_test ${test}
done
