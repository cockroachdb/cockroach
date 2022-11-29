#!/usr/bin/env bash

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

echo
echo "# Exclusions for cmds other than workload, cockroach.*"
find pkg/cmd -mindepth 1 -maxdepth 1 -type d ! -name "cockroach*" ! -name "workload"
echo
cat ${SCRIPT_DIR}/default_exclusions.txt
echo
