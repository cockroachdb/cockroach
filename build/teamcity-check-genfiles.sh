#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"
source "$(dirname "${0}")/teamcity-bazel-support.sh"  # For run_bazel

tc_prepare

tc_start_block "Ensure generated code is up-to-date"

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
check_workspace_clean "Run \`./dev generate bazel\` to automatically regenerate these."
run build/builder.sh make generate &> artifacts/generate.log || (cat artifacts/generate.log && false)
rm artifacts/generate.log
check_workspace_clean "Run \`make generate\` to automatically regenerate these."
run build/builder.sh make buildshort &> artifacts/buildshort.log || (cat artifacts/buildshort.log && false)
rm artifacts/buildshort.log
check_workspace_clean "Run \`make buildshort\` to automatically regenerate these."
tc_end_block "Ensure generated code is up-to-date"

# generated code can generate new dependencies; check dependencies after generated code.
tc_start_block "Ensure dependencies are up-to-date"
# Run go mod tidy and `make -k vendor_rebuild` and ensure nothing changes.
run build/builder.sh go mod tidy
check_workspace_clean "Run \`go mod tidy\` and \`make -k vendor_rebuild\` to automatically regenerate these."
run build/builder.sh make -k vendor_rebuild
cd vendor
check_workspace_clean "Run \`make -k vendor_rebuild\` to automatically regenerate these."
cd ..
tc_end_block "Ensure dependencies are up-to-date"

tc_start_block "Test web UI"
# Run the UI tests. This logically belongs in teamcity-test.sh, but we do it
# here to minimize total build time since this build has already generated the
# UI.
run build/builder.sh make -C pkg/ui
tc_end_block "Test web UI"
