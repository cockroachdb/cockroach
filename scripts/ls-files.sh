#!/usr/bin/env bash

# Recursively list all files in this Git checkout, including files in
# submodules.

set -euo pipefail

submodules=($(git submodule foreach --quiet 'echo $path'))

# IMPORTANT: This script must never output a bare directory. That is, given a
# directory tree with files a/1 and a/2, this script must output "a/1 \n a/2"
# and not "a/ \n a/1 \n a/2". Bare directories will cause e.g. tar to include
# the entire directory tree, then re-include the files when the files in the
# directory are listed on the following lines. These duplicate files will break
# tar extraction horribly.
#
# git ls-files gets this right with the notable exception of submodules, which
# are always output as a bare directory. We filter them out manually with the
# parameter expansion below, which prefixes every path in the submodules array
# with `:(exclude)`, resulting in a final command like:
#
#     git ls-files . :(exclude)vendor :(exclude)c-deps/jemalloc...
#
git ls-files . "${submodules[@]/#/:(exclude)}"

# Then, we list all the files *within* each submodule, without listing the bare
# submodule directory.
for submodule in "${submodules[@]}"; do
  git -C "$submodule" ls-files | sed -e "s,^,$submodule/,"
done
