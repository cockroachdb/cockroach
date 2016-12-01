#!/usr/bin/env bash

set -euo pipefail

# Glide *replaces* the vendor directory when it operates on it (e.g. in update
# or get) which has the side-effect of breaking the .git submodule pointer.
mv vendor/.git vendorgit
trap 'mv vendorgit vendor/.git' EXIT
glide "$@"
