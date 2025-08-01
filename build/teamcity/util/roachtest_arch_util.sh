# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# arch_to_config returns the bazel config for the given arch.
function arch_to_config() {
  case "$1" in
    amd64)
      echo "crosslinux"
      ;;
    arm64)
      echo "crosslinuxarm"
      ;;
    amd64-fips)
      echo "crosslinuxfips"
      ;;
    *)
      echo "Error: invalid arch '$1'" >&2
      exit 1
      ;;
  esac
}

# get_host_arch determines the current host's CPU architecture.
function get_host_arch() {
  if [[ "$(uname -m)" =~ (arm64|aarch64)$ ]]; then
    echo "arm64"
  else
    echo "amd64"
  fi
}
