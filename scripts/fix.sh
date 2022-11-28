#!/usr/bin/env bash

set -eo pipefail

source ./scripts/test_lib.sh

function mod_tidy_fix {
  rm ./go.sum
  go mod tidy || return 2
}

function go_fmt_fix {
  go fmt -n . || return 2
}

log_callout -e "\\nFixing raft code for you...\\n"

mod_tidy_fix || exit 2
go_fmt_fix || exit 2

log_success -e "\\nSUCCESS: raft code is fixed :)"

