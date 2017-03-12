#!/usr/bin/env bash

# Recursively list all files in this Git checkout, including files in
# submodules.
# TODO(benesch): Replace this with `git ls-files --recurse-submodules` once Git
# v2.11 is widely deployed.

set -euo pipefail

git ls-files
git submodule foreach --quiet --recursive 'git ls-files | sed "s|^|$path/|"'
