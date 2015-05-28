#!/bin/bash

set -eu

cd "$(dirname $0)/.."

./build/build-docker-dev.sh
./build/deploy/mkimage.sh
