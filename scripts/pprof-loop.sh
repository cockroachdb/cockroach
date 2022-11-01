#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
	echo "Usage: $0 'http://<foo>:8080/debug/pprof/profile?labels=true&seconds=5'"
	exit 1
fi

first=1
while true; do
	f="pprof_$(date -u '+%Y%m%d_%H%M%S').pb.gz"
	set +e
	# Be resilient to spurious pprof failures but make sure
	# to bail eagerly on first time since probably the URL
	# is just wrong etc.
	if ! curl --no-progress-meter "${1}" > "${f}"; then
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
	if [ -n "$(which go)" ]; then
		go tool pprof -nodefraction 0.3 -top "${f}" | head -n 15
	fi
	first=0
done

