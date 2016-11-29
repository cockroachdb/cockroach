#!/usr/bin/env bash

set -euxo pipefail

# glide likes to *replace* the 'vendor' directory when it adds or updates a dep.
# While this means that the vendor is sure what and only what is in the glide.lock
# file, it has the unfortunate side effect of nuking the .git file that makes 
# 'vendor/' a submodule, as well as the hand-written README in the root.
#
# This script restores the .git submodule pointer and then the README.
echo 'gitdir: ../.git/modules/vendor' > vendor/.git
git -C vendor checkout README.md
