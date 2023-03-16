#!/usr/bin/env bash
set -euo pipefail

if ! which pprofme; then
	echo "pprofme missing, setup instructions: https://github.com/polarsignals/pprofme#install"
	exit 1
fi

pprofme upload "$@"
