#!/usr/bin/env bash

set -xeuo pipefail

bazel run //pkg/cmd/urlcheck --config=ci --run_under="cd $PWD && "
