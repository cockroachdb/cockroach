#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"
source "$(dirname "${0}")/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Ensure generated code is up-to-date"
# Buffer noisy output and only print it on failure.
if ! run run_bazel build/bazelutil/check.sh &> artifacts/buildshort.log || (cat artifacts/buildshort.log && false); then
    # The command will output instructions on how to fix the error.
    exit 1
fi
rm artifacts/buildshort.log
TEAMCITY_BAZEL_SUPPORT_GENERATE=1  # See teamcity-bazel-support.sh.
run run_bazel build/bazelutil/bazel-generate.sh &> artifacts/buildshort.log || (cat artifacts/buildshort.log && false)
rm artifacts/buildshort.log
check_clean "Run \`make bazel-generate\` to automatically regenerate these."
run build/builder.sh make generate &> artifacts/generate.log || (cat artifacts/generate.log && false)
rm artifacts/generate.log
check_clean "Run \`make generate\` to automatically regenerate these."
run build/builder.sh make buildshort &> artifacts/buildshort.log || (cat artifacts/buildshort.log && false)
rm artifacts/buildshort.log
check_clean "Run \`make buildshort\` to automatically regenerate these."
tc_end_block "Ensure generated code is up-to-date"

# generated code can generate new dependencies; check dependencies after generated code.
tc_start_block "Ensure dependencies are up-to-date"
# Run go mod tidy and `make -k vendor_rebuild` and ensure nothing changes.
run build/builder.sh go mod tidy
check_clean "Run \`go mod tidy\` and \`make -k vendor_rebuild\` to automatically regenerate these."
run build/builder.sh make -k vendor_rebuild
cd vendor
check_clean "Run \`make -k vendor_rebuild\` to automatically regenerate these."
cd ..
tc_end_block "Ensure dependencies are up-to-date"

tc_start_block "Test web UI"
# Run the UI tests. This logically belongs in teamcity-test.sh, but we do it
# here to minimize total build time since this build has already generated the
# UI.
run build/builder.sh make -C pkg/ui
tc_end_block "Test web UI"
