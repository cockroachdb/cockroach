#!/usr/bin/env bash

set -euxo pipefail

mv vendor/.git vendorgit && \
glide up && \
mv vendorgit vendor/.git && \
git -C vendor checkout README.md
