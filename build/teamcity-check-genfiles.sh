#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"
source "$(dirname "${0}")/teamcity-bazel-support.sh"  # For run_bazel

tc_prepare

tc_start_block "Ensure generated code is up-to-date"

begin_check_generated_code_tests

# NOTE(ricky): Please make sure any changes to the Bazel-related checks here are
# propagated to build/teamcity/cockroach/ci/tests/check_generated_code_impl.sh
# as well.

# Buffer noisy output and only print it on failure.
if ! (run run_bazel build/bazelutil/check.sh &> artifacts/buildshort.log || (cat artifacts/buildshort.log && false)); then
    # The command will output instructions on how to fix the error.
    exit 1
fi
rm artifacts/buildshort.log

run run_bazel build/bazelutil/bazel-generate.sh \
  BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e COCKROACH_BAZEL_FORCE_GENERATE=1" \
  &> artifacts/buildshort.log || (cat artifacts/buildshort.log && false)

rm artifacts/buildshort.log
if grep TODO DEPS.bzl; then
    echo "Missing TODO comment in DEPS.bzl. Did you run \`./dev generate bazel --mirror\`?"
    exit 1
fi
check_workspace_clean 'dev_generate_bazel' "Run \`./dev generate bazel\` to automatically regenerate these."
run build/builder.sh make generate &> artifacts/generate.log || (cat artifacts/generate.log && false)
rm artifacts/generate.log
check_workspace_clean 'make_generate' "Run \`make generate\` to automatically regenerate these."
run build/builder.sh make buildshort &> artifacts/buildshort.log || (cat artifacts/buildshort.log && false)
rm artifacts/buildshort.log
check_workspace_clean 'make_buildshort' "Run \`make buildshort\` to automatically regenerate these."
tc_end_block "Ensure generated code is up-to-date"

# generated code can generate new dependencies; check dependencies after generated code.
tc_start_block "Ensure dependencies are up-to-date"
# Run go mod tidy and `make -k vendor_rebuild` and ensure nothing changes.
run build/builder.sh go mod tidy
check_workspace_clean 'go_mod_tidy' "Run \`go mod tidy\` and \`make -k vendor_rebuild\` to automatically regenerate these."
run build/builder.sh make -k vendor_rebuild
cd vendor
check_workspace_clean 'vendor_rebuild' "Run \`make -k vendor_rebuild\` to automatically regenerate these."
cd ..

end_check_generated_code_tests
tc_end_block "Ensure dependencies are up-to-date"
