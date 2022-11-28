#!/usr/bin/env bash

ROOT_MODULE="go.etcd.io/raft"

function set_root_dir {
  RAFT_ROOT_DIR=$(go list -f '{{.Dir}}' "${ROOT_MODULE}/v3")
}

set_root_dir

####   Convenient IO methods #####

COLOR_RED='\033[0;31m'
COLOR_ORANGE='\033[0;33m'
COLOR_GREEN='\033[0;32m'
COLOR_LIGHTCYAN='\033[0;36m'
COLOR_BLUE='\033[0;94m'
COLOR_MAGENTA='\033[95m'
COLOR_BOLD='\033[1m'
COLOR_NONE='\033[0m' # No Color


function log_error {
  >&2 echo -n -e "${COLOR_BOLD}${COLOR_RED}"
  >&2 echo "$@"
  >&2 echo -n -e "${COLOR_NONE}"
}

function log_warning {
  >&2 echo -n -e "${COLOR_ORANGE}"
  >&2 echo "$@"
  >&2 echo -n -e "${COLOR_NONE}"
}

function log_callout {
  >&2 echo -n -e "${COLOR_LIGHTCYAN}"
  >&2 echo "$@"
  >&2 echo -n -e "${COLOR_NONE}"
}

function log_cmd {
  >&2 echo -n -e "${COLOR_BLUE}"
  >&2 echo "$@"
  >&2 echo -n -e "${COLOR_NONE}"
}

function log_success {
  >&2 echo -n -e "${COLOR_GREEN}"
  >&2 echo "$@"
  >&2 echo -n -e "${COLOR_NONE}"
}

function log_info {
  >&2 echo -n -e "${COLOR_NONE}"
  >&2 echo "$@"
  >&2 echo -n -e "${COLOR_NONE}"
}

# run_for_module [module] [cmd]
# executes given command in the given module for given pkgs.
#   module_name - "." (in future: tests, client, server)
#   cmd         - cmd to be executed - that takes package as last argument
function run_for_module {
  local module=${1:-"."}
  shift 1
  (
    cd "${RAFT_ROOT_DIR}/${module}" && "$@"
  )
}

# tool_pkg_dir [pkg] - returns absolute path to a directory that stores given pkg.
# The pkg versions must be defined in ./tools/mod directory.
function tool_pkg_dir {
  run_for_module ./tools/mod go list -f '{{.Dir}}' "${1}"
}

# tool_get_bin [tool] - returns absolute path to a tool binary (or returns error)
function tool_get_bin {
  local tool="$1"
  local pkg_part="$1"
  if [[ "$tool" == *"@"* ]]; then
    pkg_part=$(echo "${tool}" | cut -d'@' -f1)
    # shellcheck disable=SC2086
    go install ${GOBINARGS:-} "${tool}" || return 2
  else
    # shellcheck disable=SC2086
    run_for_module ./tools/mod go install ${GOBINARGS:-} "${tool}" || return 2
  fi

  # remove the version suffix, such as removing "/v3" from "go.etcd.io/etcd/v3".
  local cmd_base_name
  cmd_base_name=$(basename "${pkg_part}")
  if [[ ${cmd_base_name} =~ ^v[0-9]*$ ]]; then
    pkg_part=$(dirname "${pkg_part}")
  fi

  run_for_module ./tools/mod go list -f '{{.Target}}' "${pkg_part}"
}

# tool_get_bin [tool]
function run_go_tool {
  local cmdbin
  if ! cmdbin=$(GOARCH="" tool_get_bin "${1}"); then
    log_warning "Failed to install tool '${1}'"
    return 2
  fi
  shift 1
  GOARCH="" "${cmdbin}" "$@" || return 2
}
