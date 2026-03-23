#!/usr/bin/env bash
#
# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
set -euo pipefail

# These values are substituted in the Go code that uses this. We use custom
# delimiters for the Go text template so that this bash script is valid
# before template evaluation (so we can use shellcheck).
LOCAL=#{if .Local#}true#{end#}
LOG_DIR=#{shesc .LogDir#}
BINARY=#{shesc .Binary#}
MEMORY_MAX=#{.MemoryMax#}
NUM_FILES_LIMIT=#{.NumFilesLimit#}
VIRTUAL_CLUSTER_LABEL=#{.VirtualClusterLabel#}
#{if .AutoRestart#}
AUTO_RESTART=1
#{end#}

ARGS=(
#{range .Args -#}
#{shesc .#}
#{end -#}
)
ENV_VARS=(
#{range .EnvVars -#}
#{shesc .#}
#{end -#}
)

# End of templated code.

if [[ -n "${LOCAL}" || "${1-}" == "run" ]]; then
  mkdir -p "${LOG_DIR}"
  echo "sqlproxy start: $(date), logging to ${LOG_DIR}" | tee -a "${LOG_DIR}"/{roachprod,cockroach.std{out,err}}.log
  # NB: ENV_VARS is never empty.
  export "${ENV_VARS[@]}"
  CODE=0

  if [[ -n "${LOCAL}" ]]; then
    # For local clusters, run in background with a PID file
    # NB: mt start-proxy doesn't support --background flag like cockroach does
    PID_FILE="${LOG_DIR}/cockroach.pid"
    rm -f "${PID_FILE}"
    ROACHPROD_VIRTUAL_CLUSTER="${VIRTUAL_CLUSTER_LABEL}" "${BINARY}" "${ARGS[@]}" >> "${LOG_DIR}/cockroach.stdout.log" 2>> "${LOG_DIR}/cockroach.stderr.log" &
    PID=$!
    echo ${PID} > "${PID_FILE}"
    echo "sqlproxy started with PID ${PID}: $(date)" | tee -a "${LOG_DIR}/roachprod.log"
    exit 0
  else
    ROACHPROD_VIRTUAL_CLUSTER="${VIRTUAL_CLUSTER_LABEL}" "${BINARY}" "${ARGS[@]}" >> "${LOG_DIR}/cockroach.stdout.log" 2>> "${LOG_DIR}/cockroach.stderr.log" || CODE="$?"
    echo "sqlproxy exited with code ${CODE}: $(date)" | tee -a "${LOG_DIR}"/{roachprod,cockroach.{exit,std{out,err}}}.log
    exit "${CODE}"
  fi
fi

# Set up systemd unit and start it, which will recursively
# invoke this script but hit the above conditional.

if systemctl is-active -q "${VIRTUAL_CLUSTER_LABEL}"; then
  echo "${VIRTUAL_CLUSTER_LABEL} service already active"
  echo "To get more information: systemctl status ${VIRTUAL_CLUSTER_LABEL}"
  exit 1
fi

# If the proxy failed, the service still exists; we need to clean it up before
# we can start it again.
sudo systemctl reset-failed "${VIRTUAL_CLUSTER_LABEL}" 2>/dev/null || true

# The first time we run, install a small script that shows some helpful
# information when we ssh in.
if [ ! -e "${HOME}/.profile-${VIRTUAL_CLUSTER_LABEL}" ]; then
  cat > "${HOME}/.profile-${VIRTUAL_CLUSTER_LABEL}" <<EOQ
echo ""
if systemctl is-active -q ${VIRTUAL_CLUSTER_LABEL}; then
  echo "${VIRTUAL_CLUSTER_LABEL} is running; see: systemctl status ${VIRTUAL_CLUSTER_LABEL}"
elif systemctl is-failed -q ${VIRTUAL_CLUSTER_LABEL}; then
  echo "${VIRTUAL_CLUSTER_LABEL} stopped; see: systemctl status ${VIRTUAL_CLUSTER_LABEL}"
else
  echo "${VIRTUAL_CLUSTER_LABEL} not started"
fi
echo ""
EOQ
  echo ". ${HOME}/.profile-${VIRTUAL_CLUSTER_LABEL}" >> "${HOME}/.profile"
fi

# We run this script (with arg "run") as a service unit. We do not use --user
# because memory limiting doesn't work in that mode. Instead we pass the uid and
# gid that the process will run under.
# The "notify" service type is not needed for sqlproxy as it doesn't support
# systemd notifications.
sudo systemd-run --unit "${VIRTUAL_CLUSTER_LABEL}" \
  --same-dir --uid "$(id -u)" --gid "$(id -g)" \
  -p "MemoryMax=${MEMORY_MAX}" \
  -p LimitCORE=infinity \
  -p "LimitNOFILE=${NUM_FILES_LIMIT}" \
  ${AUTO_RESTART:+-p Restart=always -p RestartSec=5s -p StartLimitIntervalSec=60s -p StartLimitBurst=3} \
  bash "${0}" run
