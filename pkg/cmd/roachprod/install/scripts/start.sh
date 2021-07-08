#!/usr/bin/env bash
#
# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.
set -euo pipefail

# These values are substituted in the Go code that uses this.
LOCAL="{{if .Local}}true{{end}}"
LOG_DIR='{{printf "%q" .LogDir}}'
NODE_NUM="{{.NodeNum}}"
TAG="{{.Tag}}"
ENV_VARS="{{.EnvVars}}"
BINARY="{{.Binary}}"
START_CMD="{{.StartCmd}}"
KEY_CMD="{{.KeyCmd}}"
MEMORY_MAX="{{.MemoryMax}}"
# TODO(during review): couldn't figure this one out - this has both single and
# double quotes in it and isn't there a general way to do this better?
# ARGS='{{.Args}}'

mkdir -p "${LOG_DIR}"

if [[ -n "${LOCAL}" || "${1-}" == "run" ]]; then
  mkdir -p "${LOG_DIR}"
  echo "cockroach start: $(date), logging to ${LOG_DIR} | tee -a ${LOG_DIR}/roachprod,cockroach.std{out,err}}.log"
  if [[ -n "${KEY_CMD}" ]]; then
    "${KEY_CMD}"
  fi
  export ROACHPROD="${NODE_NUM}}${TAG} ${ENV_VARS}"
  background=""
  if [[ -n "${LOCAL}" ]]; then
    background="--background"
  fi
  CODE=0
  "${BINARY}" ${START_CMD} {{.Args}} ${background} >> "${LOG_DIR}/cockroach.stdout.log" 2>> "${LOG_DIR}/cockroach.stderr.log" || CODE=$?
  if [[ -z "${LOCAL}" || "${CODE}" -ne 0 ]]; then
    echo "cockroach exited with code ${CODE}: $(date)" | tee -a "${LOG_DIR}/{roachprod,cockroach.{exit,std{out,err}}}.log"
  fi
  exit "${CODE}"
fi

# Set up systemd unit and start it, which will recursively
# invoke this script but hit the above conditional.

if systemctl is-active -q cockroach; then
  echo "cockroach service already active"
  echo "To get more information: systemctl status cockroach"
  exit 1
fi

# If cockroach failed, the service still exists; we need to clean it up before
# we can start it again.
sudo systemctl reset-failed cockroach 2>/dev/null || true

# The first time we run, install a small script that shows some helpful
# information when we ssh in.
if [ ! -e ${HOME}/.profile-cockroach ]; then
  cat > ${HOME}/.profile-cockroach <<'EOQ'
echo ""
if systemctl is-active -q cockroach; then
  echo "cockroach is running; see: systemctl status cockroach"
elif systemctl is-failed -q cockroach; then
  echo "cockroach stopped; see: systemctl status cockroach"
else
  echo "cockroach not started"
fi
echo ""
EOQ
  echo ". ${HOME}/.profile-cockroach" >> ${HOME}/.profile
fi

# We run this script (with arg "run") as a service unit. We do not use --user
# because memory limiting doesn't work in that mode. Instead we pass the uid and
# gid that the process will run under.
# The "notify" service type means that systemd-run waits until cockroach
# notifies systemd that it is ready; NotifyAccess=all is needed because this
# notification doesn't come from the main PID (which is bash).
sudo systemd-run --unit cockroach \
  --same-dir --uid $(id -u) --gid $(id -g) \
  --service-type=notify -p NotifyAccess=all \
  -p "MemoryMax=${MEMORY_MAX}" \
  -p LimitCORE=infinity \
  -p LimitNOFILE=65536 \
  bash $0 run
