#!/usr/bin/env bash

# Copyright 2016 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# Script that converts a log file to html, using cockroach-specific log syntax
# highlighting. Requires vim to be installed.
#
# Usage: log2html.sh [input-file [output-file]]
#          input-file can be - for stdin.
#          If not specified, stdin/stdout are used.

set -e

input="$1"
[ -z "$input" ] && input="-"

output="$2"

outfile="$output"
if [ -z "$output" ]; then
  outfile=$(mktemp)
  trap "rm -f $outfile" EXIT
fi

dir=$(dirname $0)

if ! $(which vim >/dev/null 2>/dev/null); then
  >&2 echo "Error: vim needs to be installed."
  >&2 echo "  on Ubuntu: sudo apt-get install vim-gtk"
  >&2 echo "  on Mac: brew install vim"
  exit 1
fi

# Check if vim supports the "--not-a-term" switch which suppresses a warning
# about the output not being a terminal.
EXTRA_OPTS=""
if vim --not-a-term --version >/dev/null 2>/dev/null; then
  EXTRA_OPTS="--not-a-term"
fi

vim -X $EXTRA_OPTS -n -i NONE -u NORC \
  +"syn on" \
  +"colorscheme torte" \
  +"source $dir/crlog.vim" \
  +"let g:html_no_progress=1" \
  +"run! syntax/2html.vim" \
  +"saveas! $outfile" \
  +"qa!" \
  "$input" >/dev/null

if [ -z "$output" ]; then
  cat $outfile
fi
