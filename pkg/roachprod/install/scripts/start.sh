#!/usr/bin/env bash
#
# Copyright 2021 The Cockroach Authors.
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
KEY_CMD=#{.KeyCmd#}
MEMORY_MAX=#{.MemoryMax#}
NUM_FILES_LIMIT=#{.NumFilesLimit#}
VIRTUAL_CLUSTER_LABEL=#{.VirtualClusterLabel#}
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

if [[ -n "${LOCAL}" ]]; then
  # Write to an empty pid file. This is referenced by the roachprod
  # monitor to find the PID of cockroach processes running locally.
  PID_FILE="${LOG_DIR}/cockroach.pid"
  rm -f "${PID_FILE}"
  ARGS+=("--background" "--pid-file" "${PID_FILE}")
fi

if [[ -n "${LOCAL}" || "${1-}" == "run" ]]; then
  mkdir -p "${LOG_DIR}"
  echo "cockroach start: $(date), logging to ${LOG_DIR}" | tee -a "${LOG_DIR}"/{roachprod,cockroach.std{out,err}}.log
  if [[ -n "${KEY_CMD}" ]]; then
    eval "${KEY_CMD}"
  fi
  # NB: ENV_VARS is never empty.
  export "${ENV_VARS[@]}"
  # If we are collecting code coverage, make sure the directory exists.
  if [ -n "${BAZEL_COVER_DIR:-}" ]; then
    mkdir -p "${BAZEL_COVER_DIR}"
  fi
  CODE=0
  ROACHPROD_VIRTUAL_CLUSTER="${VIRTUAL_CLUSTER_LABEL}" "${BINARY}" "${ARGS[@]}" >> "${LOG_DIR}/cockroach.stdout.log" 2>> "${LOG_DIR}/cockroach.stderr.log" || CODE="$?"
  if [[ -z "${LOCAL}" || "${CODE}" -ne 0 ]]; then
    echo "cockroach exited with code ${CODE}: $(date)" | tee -a "${LOG_DIR}"/{roachprod,cockroach.{exit,std{out,err}}}.log
  fi
  exit "${CODE}"
fi

# Set up systemd unit and start it, which will recursively
# invoke this script but hit the above conditional.

if systemctl is-active -q "${VIRTUAL_CLUSTER_LABEL}"; then
  echo "${VIRTUAL_CLUSTER_LABEL} service already active"
  echo "To get more information: systemctl status ${VIRTUAL_CLUSTER_LABEL}"
  exit 1
fi

# If cockroach failed, the service still exists; we need to clean it up before
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
# The "notify" service type means that systemd-run waits until cockroach
# notifies systemd that it is ready; NotifyAccess=all is needed because this
# notification doesn't come from the main PID (which is bash).
sudo systemd-run --unit "${VIRTUAL_CLUSTER_LABEL}" \
  --same-dir --uid "$(id -u)" --gid "$(id -g)" \
  --service-type=notify -p NotifyAccess=all \
  -p "MemoryMax=${MEMORY_MAX}" \
  -p LimitCORE=infinity \
  -p "LimitNOFILE=${NUM_FILES_LIMIT}" \
  bash "${0}" run
