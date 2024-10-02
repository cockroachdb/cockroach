#!/usr/bin/env bash
#
# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
set -euo pipefail

start_port=#{.StartPort#}
port_count=#{.PortCount#}

open_ports=()
ports_found=0

set +e
for ((port = start_port; port < 32768; port++)); do
  if ! lsof -i :"$port" >/dev/null 2>&1; then
    open_ports+=("$port")
    ((ports_found++))

    if ((ports_found >= port_count)); then
      break
    fi
  fi
done

set -e
if ((ports_found > 0)); then
  echo "${open_ports[@]}"
else
  echo "no open ports found" >&2
  exit 1
fi
