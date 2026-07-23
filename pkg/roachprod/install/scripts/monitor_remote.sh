#!/usr/bin/env bash
#
# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script is used to monitor the status of cockroach processes on a remote
# node where systemctl is available.
# It produces output in the following format:
#cockroach-system=500
#status=unknown
#cockroach-tenant_0=501
#status=1
#\n = end of frame

one_shot=#{if .OneShot#}true#{end#}

prev_frame=""
while :; do
  # Get all cockroach system units
  sysctl_output=$(systemctl list-units cockroach\*.service --type=service --no-legend --no-pager --plain | awk '{print $1}')
  frame=""
  while IFS= read -r name; do
    # Query the PID and status of the cockroach system unit
    pid=$(systemctl show "$name" --property MainPID --value)
    status=$(systemctl show "$name" --property ExecMainStatus --value)
    vc_label=${name%.service}
    frame+="$vc_label=$pid\n"
    frame+="status=$status\n"
  done <<< "$sysctl_output"
  # Only print the frame if it has changed.
  if [ "$frame" != "$prev_frame" ]; then
    echo -e "$frame"
    prev_frame="$frame"
  fi
  # If one_shot is set, exit after the first iteration.
  if [[ -n "${one_shot}" ]]; then
    break
  fi
  sleep 1
done
