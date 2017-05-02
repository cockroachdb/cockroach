#!/usr/bin/env bash

set -euxo pipefail

build/builder.sh make archive ARCHIVE=build/archive-verifier/cockroach.src.tgz
docker build ./build/archive-verifier
