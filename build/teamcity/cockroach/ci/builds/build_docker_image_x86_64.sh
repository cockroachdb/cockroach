#!/usr/bin/env bash
set -euxo pipefail

source "$(dirname "${0}")/build_docker_image.sh" amd64
