#!/usr/bin/env bash
#
# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script is used to monitor the status of cockroach processes on local nodes
# using `ps`. It does not support checking the exit status of a process, but
# still outputs the status line for consistency with the remote monitor.
# It produces output in the following format:
#cockroach-system=500
#status=unknown
#cockroach-tenant_0=501
#status=unknown
#\n = end of frame

roachprod_regex=#{shesc .RoachprodEnvRegex#}
one_shot=#{if .OneShot#}true#{end#}

prev_frame=""
while :; do
  # Get the PID and command of all processes that match the roachprod regex.
  ps_output=$(ps axeww -o pid,command | grep -v grep | grep -E "$roachprod_regex")
  frame=""
  while IFS= read -r line; do
    # Extract the PID and command from the line.
    read -r pid command <<< "$line"
    # If the command contains the `--background` flag, skip it. It's the command
    # that is starting the background cockroach process.
    if [[ "$command" == *"--background"* ]]; then
      continue
    fi
    # Extract the virtual cluster label from the command.
    vc_label=$(echo "$command" | grep -E -o 'ROACHPROD_VIRTUAL_CLUSTER=[^ ]*' | cut -d= -f2)
    # If the virtual cluster label is not empty, print the label and the PID.
    # Also print the status of the process, if remote, where systemd is available.
    if [ -n "$vc_label" ]; then
      frame+="$vc_label=$pid\n"
      # If the process is local we can't check the status (exit code).
      frame+="status=unknown\n"
    fi
  done <<< "$ps_output"
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
