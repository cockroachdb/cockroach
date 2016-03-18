#!/bin/bash
# The caller should download the static-tests and extract them into the
# local directory.

set -x

LOG_DIR="logs"
MAX_RUNS=0
MAX_TIME=15m
MAX_FAILS=1

STRESS_FLAGS="-maxruns ${MAX_RUNS} -maxtime ${MAX_TIME} -maxfails ${MAX_FAILS} -stderr"
TEST_FLAGS="-test.v"

# Enable dumping of all goroutine stacks due to unrecovered panics.
export GOTRACEBACK=all

# takes the full path to the test binary.
function run_one_test {
  test_binary=$1
  name=$(basename ${test_binary})
  package=$(dirname ${test_binary})
  cur_dir=$(pwd -P)
  log_dir="${cur_dir}/${LOG_DIR}/${package}"
  mkdir -p "${log_dir}"
  echo "${name}: ${test_binary} ${log_dir}"
  # Each test must be run from the respective package directory
  stress_bin="${cur_dir}/stress"
  pushd ${package}
  ${stress_bin} ${STRESS_FLAGS} ./${name} ${TEST_FLAGS} 2> ${log_dir}/${name}.stderr > ${log_dir}/${name}.stdout
  popd
}

for test in $(find cockroach/ -type f -name '*.test' | sort); do
  run_one_test ${test}
done
