#!/usr/bin/env bash
#
# reports.sh generates reports about the finite state machine.
#
# Usage: reports.sh <exported Transitions variable> <starting state name>
#
# Example: reports.sh TxnStateTransitions stateNoTxn

set -euo pipefail

# Determine the current package name.
full_pkg=$(go list)
base_pkg=$(basename "$full_pkg")

type="$1"
lower_type=$(echo "$1" | awk '{print tolower($0)}')
start_state="$2"

# Substitute variables in template .go file.
dir=$(mktemp -d)
write_reports=$dir/write_reports.go
write_reports_tmpl=$(dirname "$0")/write_reports.go.tmpl
sed -e "s~{tmpl-full-pkg}~$full_pkg~"\
    -e "s~{tmpl-base-pkg}~$base_pkg~"\
    -e "s~{tmpl-type}~$type~"\
    -e "s~{tmpl-start-state}~$start_state~"\
    "$write_reports_tmpl" > "$write_reports"

# Run .go file to generate reports.
diagram_file="$lower_type"_diagram.gv
report_file="$lower_type"_report.txt
go run "$write_reports" "$diagram_file" "$report_file" "$0 $*"
