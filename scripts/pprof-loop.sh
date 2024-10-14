#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -eu

if [ "$#" -ne 1 ]; then
	cat <<EOF
Takes profiles or runtime traces in a loop. For endpoints that don't
block, fetches at 1s intervals.

See https://pkg.go.dev/runtime/pprof for details.

For secure clusters, invoke this script with an auth cookie in the
PPROF_LOOP_COOKIE env var. A cookie can be obtained via:

  cockroach auth-session login root \
    --certs-dir=certs --only-cookie --expire-after 24h

Usage:

$0 'http://localhost:8080/debug/pprof/allocs'
$0 'http://localhost:8080/debug/pprof/heap'
$0 'http://localhost:8080/debug/pprof/mutex'
$0 'http://localhost:8080/debug/pprof/goroutine'

# not sampled by default; need COCKROACH_BLOCK_PROFILE_RATE
$0 'http://localhost:8080/debug/pprof/block'

# allocs within the last 3s.
$0 'http://localhost:8080/debug/pprof/allocs?seconds=3'

# 3s runtime traces
$0 'http://localhost:8080/debug/pprof/trace?seconds=3'

# 3s CPU profiles
$0 'http://localhost:8080/debug/pprof/profile?seconds=3'
EOF
	exit 1
fi

first=1
extra_sleep=0
while true; do
	f="pprof_$(date -u '+%Y%m%d_%H%M%S').pb.gz"
	if [ -f "${f}" ]; then
		# If we ever see ourselves overwriting the
		# same file, we got through the loop twice
		# in one second. Add an extra one second sleep
		# for all remaining loops.
		#
		# This makes this script "just work" with
		# non-blocking profiles such as heap and mutex.
		extra_sleep=1
	fi
	set +e
	# Be resilient to spurious pprof failures but make sure
	# to bail eagerly on first time since probably the URL
	# is just wrong etc.
	if ! curl -k --cookie "${PPROF_LOOP_COOKIE-}" --no-progress-meter "${1}" > "${f}"; then
		if [ $first -eq 1 ]; then
			exit 1
		fi
		# Remove garbage files, back off, try again.
		rm "${f}"
		sleep 1
		continue
	fi
	set -e
	echo "${f}"
	if [[ -n "$(which go)" && "${1}" != *"/trace"* ]]; then
		go tool pprof -nodefraction 0.3 -top "${f}" | head -n 15
	fi
	first=0
	sleep "${extra_sleep}"
done

