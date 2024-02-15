#!/usr/bin/env bash

# Any arguments passed to this script will be directly passed to Bazel. In
# addition to just calling Bazel, this script has the property that the Bazel
# server will be killed if we receive an interrupt.

kill_bazel() {
    PIDS=$(ps -ef | grep bazel | grep -v run-bazel.sh | grep -v grep | awk '{print $2}')
    for PID in $PIDS
    do
        kill -KILL $PID
    done
}

trap kill_bazel INT TERM

bazel "$@"
