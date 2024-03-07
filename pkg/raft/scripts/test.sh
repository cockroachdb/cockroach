#!/usr/bin/env bash

set -eo pipefail

source ./scripts/test_lib.sh

# set default GOARCH if not set
if [ -z "$GOARCH" ]; then
  GOARCH=$(go env GOARCH);
fi

# determine whether target supports race detection
if [ -z "${RACE}" ] ; then
  if [ "$GOARCH" == "amd64" ]; then
    RACE="--race"
  else
    RACE="--race=false"
  fi
else
  RACE="--race=${RACE:-true}"
fi

# This options make sense for cases where SUT (System Under Test) is compiled by test.
COMMON_TEST_FLAGS=("${RACE}")
if [[ -n "${CPU}" ]]; then
  COMMON_TEST_FLAGS+=("--cpu=${CPU}")
fi

######### Code formatting checkers #############################################

# generic_checker [cmd...]
# executes given command in the current module, and clearly fails if it
# failed or returned output.
function generic_checker {
  local cmd=("$@")
  if ! output=$("${cmd[@]}"); then
    echo "${output}"
    log_error -e "FAIL: '${cmd[*]}' checking failed (!=0 return code)"
    return 255
  fi
  if [ -n "${output}" ]; then
    echo "${output}"
    log_error -e "FAIL: '${cmd[*]}' checking failed (printed output)"
    return 255
  fi
}

function go_fmt_for_package {
  # We utilize 'go fmt' to find all files suitable for formatting,
  # but reuse full power gofmt to perform just RO check.
  go fmt -n . | sed 's| -w | -d |g' | sh
}

function gofmt_pass {
  generic_checker go_fmt_for_package
}

function genproto_pass {
  "${RAFT_ROOT_DIR}/scripts/verify_genproto.sh"
}

######## VARIOUS CHECKERS ######################################################

function dump_deps_of_module() {
  local module
  if ! module=$(go list -m); then
    return 255
  fi
  go list -f "{{if not .Indirect}}{{if .Version}}{{.Path}},{{.Version}},${module}{{end}}{{end}}" -m all
}

# Checks whether dependencies are consistent across modules
function dep_pass {
  local all_dependencies
  all_dependencies=$(dump_deps_of_module | sort) || return 2

  local duplicates
  duplicates=$(echo "${all_dependencies}" | cut -d ',' -f 1,2 | sort | uniq | cut -d ',' -f 1 | sort | uniq -d) || return 2

  for dup in ${duplicates}; do
    log_error "FAIL: inconsistent versions for depencency: ${dup}"
    echo "${all_dependencies}" | grep "${dup}" | sed "s|\\([^,]*\\),\\([^,]*\\),\\([^,]*\\)|  - \\1@\\2 from: \\3|g"
  done
  if [[ -n "${duplicates}" ]]; then
    log_error "FAIL: inconsistent dependencies"
    return 2
  else
    log_success "SUCCESS: dependencies are consistent across modules"
  fi
}

function mod_tidy_for_module {
  # Watch for upstream solution: https://github.com/golang/go/issues/27005
  local tmpModDir
  tmpModDir=$(mktemp -d -t 'tmpModDir.XXXXXX')
  cp "./go.mod" "${tmpModDir}" || return 2

  # Guarantees keeping go.sum minimal
  # If this is causing too much problems, we should
  # stop controlling go.sum at all.
  rm go.sum
  go mod tidy || return 2

  set +e
  local tmpFileGoModInSync
  diff -C 5 "${tmpModDir}/go.mod" "./go.mod"
  tmpFileGoModInSync="$?"

  # Bring back initial state
  mv "${tmpModDir}/go.mod" "./go.mod"

  if [ "${tmpFileGoModInSync}" -ne 0 ]; then
    log_error "${PWD}/go.mod is not in sync with 'go mod tidy'"
    return 255
  fi
}

function mod_tidy_pass {
  mod_tidy_for_module
}

################# REGULAR TESTS ################################################

# run_unit_tests [pkgs] runs unit tests for a current module and givesn set of [pkgs]
function run_unit_tests {
  shift 1
  # shellcheck disable=SC2086
  GOLANG_TEST_SHORT=true go test ./... -short -timeout="${TIMEOUT:-3m}" "${COMMON_TEST_FLAGS[@]}" "${RUN_ARG[@]}" "$@"
}

function unit_pass {
  run_unit_tests "$@"
}

########### MAIN ###############################################################

function run_pass {
  local pass="${1}"
  shift 1
  log_callout -e "\\n'${pass}' started at $(date)"
  if "${pass}_pass" "$@" ; then
    log_success "'${pass}' completed at $(date)"
  else
    log_error "FAIL: '${pass}' failed at $(date)"
    exit 255
  fi
}

log_callout "Starting at: $(date)"
for pass in $PASSES; do
  run_pass "${pass}" "${@}"
done

log_success "SUCCESS"

