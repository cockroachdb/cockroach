#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "compile c dependencies"
# buffer noisy output and only print it on failure.
run build/builder.sh make -otarget c-deps &> artifacts/c-build.log || (cat artifacts/c-build.log && false)
rm artifacts/c-build.log
tc_end_block "compile c dependencies"

tc_start_block "check merge skew"
run build/builder.sh make test TESTS=-
tc_end_block "check merge skew"

tc_start_block "run c++ tests"
# buffer noisy output and only print it on failure.
run build/builder.sh make check-libroach &> artifacts/c-tests.log || (cat artifacts/c-tests.log && false)
tc_end_block "run c++ tests"