#!/usr/bin/env bash

set -euo pipefail

mv vendor/.git vendorgit

function fixvendor() {
  mv vendorgit vendor/.git && git -C vendor checkout README.md
}
trap fixvendor EXIT

glide "$@"
