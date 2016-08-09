#!/usr/bin/env bash
#
# This script wraps "go tool pprof" (and stunnel) to work better with
# HTTPS servers using untrusted certificates (like cockroachdb). "go
# tool pprof" has no option to ignore certificate errors, so we setup
# stunnel forwarding to make the server look like unencrypted HTTP.

set -euo pipefail

server=$1
if [ -z "${server}" ]; then
  echo "host:port not specified, run with: $0 host:port [profile_type]"
  exit 1
fi

profile_type=${2-profile}

port=8089

pidfile=$(mktemp /tmp/pprof.XXXXXX)
stunnel -fd 0 <<EOF
pid=${pidfile}
[http]
client = yes
accept = 127.0.0.1:${port}
connect = $1
EOF

cleanup() {
    kill "$(cat ${pidfile})"
    # stunnel cleans up its own pidfile on exit
}

trap cleanup EXIT

go tool pprof "http://127.0.0.1:${port}/debug/pprof/${profile_type}"
