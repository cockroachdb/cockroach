#!/bin/bash
cd "$(dirname $0)/.."

# Verify docker installation.
source "./build/build-docker-dev.sh"
./build/deploy/mkimage.sh
