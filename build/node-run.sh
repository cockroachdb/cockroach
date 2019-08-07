#!/usr/bin/env bash

# node-run.sh runs a command installed by NPM/Yarn. It looks for COMMAND in
# ./node_modules/bin, where NPM/Yarn install commands by default, then invokes
# it with the specified ARGS, if any.
#
# The reason for this script's existence is somewhat arcane. NodeJS strongly
# prefers non-blocking I/O, so if it detects that stdout or stderr is a Unix
# pipe, it will enable non-blocking I/O on that file descriptor [0]. The
# non-blocking I/O mode applies to every process that has a reference to that
# open file description [1]. Normally, this is not a problem: if stdout or
# stderr is a pipe, it is typically a single-purpose pipe created by a shell
# pipeline that is not shared with any other processes. In Docker, however, the
# stdout and stderr streams are named FIFOs, which look like anonymous pipes but
# are shared by all processes launched by the same shell. Launching a NodeJS
# process in Docker will thus make stdout and stderr non-blocking for all other
# processes invoked by the same shell.
#
# In CI, because we run NodeJS and `go test` in the same Docker shell, `go test`
# will intermittently drop output on the floor when a write fails with EAGAIN.
# `go-test-teamcity` interprets this missing output as an indication that the
# test panicked. Note that dropping output is reasonable behavior from `go
# test`--most programs simply cannot and should not handle non-blocking stdio
# streams [2]--and is arguably a bug in NodeJS [3].
#
# As a workaround, this script pipes NodeJS's stdout and stderr through `cat`,
# which prevents the actual stdout and stderr streams from being infected with
# non-blocking I/O.
#
# TODO(benesch): see if we can propagate isatty from the true stdout and stderr
# so that colors, etc. are supported.

# [0]: https://nodejs.org/docs/latest-v8.x/api/process.html#process_a_note_on_process_i_o
# [1]: See `man 2 open` for details on the difference between a file
#      descriptor, an open file description, and a file.
# [2]: https://jdebp.eu/FGA/dont-set-shared-file-descriptors-to-non-blocking-mode.html
# [3]: https://github.com/nodejs/node/issues/14752

set -euo pipefail

[[ "${1-}" ]] || { echo "usage: $0 [-C CWD] COMMAND [ARGS...]" >&2; exit 1; }

while getopts "C:" opt; do
  case $opt in
     C) cd "$OPTARG" ;;
    \?) exit 1;
  esac
done
shift $((OPTIND-1))

"$@" > >(cat) 2> >(cat >&2)
