#!/bin/bash
set -euo pipefail

mkdir -p {{.LogDir}}
helper="{{if .Local}}{{.LogDir}}{{else}}${HOME}{{end}}/cockroach-helper.sh"
verb="{{if .Local}}run{{else}}run-systemd{{end}}"

# 'EOF' disables parameter substitution in the heredoc.
cat > "${helper}" << 'EOF' && chmod +x "${helper}" && "${helper}" "${verb}"
#!/bin/bash
set -euo pipefail

if [[ "${1}" == "run" ]]; then
  local="{{if .Local}}true{{end}}"
  mkdir -p {{.LogDir}}
  echo "cockroach start: $(date), logging to {{.LogDir}}" | tee -a {{.LogDir}}/{roachprod,cockroach.std{out,err}}.log
  {{.KeyCmd}}
  export ROACHPROD={{.NodeNum}}{{.Tag}} {{.EnvVars}}
  background=""
  if [[ "${local}" ]]; then
    background="--background"
  fi
  CODE=0
  {{.Binary}} {{.StartCmd}} {{.Args}} ${background} >> {{.LogDir}}/cockroach.stdout.log 2>> {{.LogDir}}/cockroach.stderr.log || CODE=$?
  if [[ -z "${local}" || ${CODE} -ne 0 ]]; then
    echo "cockroach exited with code ${CODE}: $(date)" | tee -a {{.LogDir}}/{roachprod,cockroach.{exit,std{out,err}}}.log
  fi
  exit ${CODE}
fi

if [[ "${1}" != "run-systemd" ]]; then
  echo "unsupported: ${1}"
  exit 1
fi

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
  -p MemoryMax={{.MemoryMax}} \
  -p LimitCORE=infinity \
  -p LimitNOFILE=65536 \
	bash $0 run
EOF
