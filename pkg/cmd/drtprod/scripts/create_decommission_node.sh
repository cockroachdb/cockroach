#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This creates a script that runs decommission for the dead nodes at 5AM everyday

if [ -z "${CLUSTER}" ]; then
  echo "environment CLUSTER is not set"
  exit 1
fi

absolute_path=$(drtprod run "${CLUSTER}":1 -- "realpath ./cockroach")
pwd=$(drtprod run "${CLUSTER}":1 -- "dirname ${absolute_path}")

drtprod ssh "${CLUSTER}":1 -- "tee decommission.sh > /dev/null << 'EOF'
#!/bin/bash

# script is responsible for decommissioning preempted nodes

# This runs the cockroach node status command and capture the output
output=\$(${pwd}/cockroach node status --certs-dir=./certs --all | tr -d ' ' | awk 'NR>1 && \$24==\"active\" && \$8==\"false\" && \$9==\"false\" {print \$1}' | paste -s -d ' ')

# It checks if any nodes were found
if [ -z \"\$output\" ]; then
  echo \"No nodes found for decommissioning.\"
else
  echo \"Decommissioning nodes: \$output\"

  # Then, it runs the cockroach node decommission command with the nodes from the output
  ${pwd}/cockroach node decommission \$output --certs-dir=./certs
fi
EOF"
drtprod ssh "${CLUSTER}":1 -- "chmod +x decommission.sh"
# this step adds the entry cronjob to run everyday at 5 AM
drtprod run "${CLUSTER}":1 -- "(crontab -l; echo \"0 5 * * * ${pwd}/decommission.sh\") | crontab -"
# these steps unmasks and starts cron
drtprod ssh "${CLUSTER}":1 -- sudo systemctl unmask cron
drtprod ssh "${CLUSTER}":1 -- sudo systemctl start cron


