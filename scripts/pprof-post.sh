#!/usr/bin/env bash
set -euo pipefail

# Usage: $0 "optional description, defaults to current date" < somefile.pb.gz
out=$(curl -s 'https://api.polarsignals.com/api/share/v1/profiles' \
	-X POST \
	-F "description=${1-$(date -u)}" \
	-F "profile=@-")
echo -n "https://share.polarsignals.com/"
grep -Eo '[0-9a-f]{4,}' <<< "${out}"
	
