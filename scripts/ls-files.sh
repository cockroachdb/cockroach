#!/usr/bin/env bash

# Recursively list all files in this Git checkout, including files in
# submodules.

set -euo pipefail

# IMPORTANT: This script must never output a bare directory. That is, given a
# directory tree with files a/1 and a/2, this script must output "a/1 \n a/2"
# and not "a/ \n a/1 \n a/2". Bare directories will cause e.g. tar to include
# the entire directory tree, then re-include the files when the files in the
# directory are listed on the following lines. These duplicate files will break
# tar extraction horribly.
#
# git ls-files gets this right with the notable exception of submodules, which
# are always output as a bare directory. We filter them out manually.
git ls-files . ':!vendor'
git -C vendor ls-files | sed -e s,^,vendor/,
