#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

# Check that a server started with only in-memory stores automatically
# logs to stderr.
send "$argv start --insecure --store=type=mem,size=1GiB\r"
eexpect "CockroachDB"
eexpect "starting cockroach node"

# Stop it.
interrupt
eexpect ":/# "

# Disable replication so as to avoid spurious purgatory errors.
start_server $argv
send "$argv zone set .default --disable-replication\r"
eexpect "num_replicas: 1"
eexpect ":/# "
stop_server $argv

# Check that a server started with --logtostderr
# logs even info messages to stderr.
send "$argv start --insecure --logtostderr\r"
eexpect "CockroachDB"
eexpect "starting cockroach node"

# Stop it.
interrupt
eexpect ":/# "

# Check that --logtostderr can override the threshold but no error is printed on startup
send "echo marker; $argv start --insecure --logtostderr=ERROR 2>&1 | grep -v '^\\*'\r"
eexpect "marker\r\nCockroachDB node starting"

# Stop it.
interrupt
eexpect ":/# "

start_server $argv

# Now test `quit` as non-start command, and test that `quit` does not
# emit logging output between the point the command starts until it
# either prints the final ok message or fails with some error
# (e.g. due to no definite answer from the server).
send "echo marker; $argv quit 2>&1 | grep '^\\(\[IWEF\]\[0-9\]\\)' \r"
set timeout 20
eexpect "marker\r\n:/# "
set timeout 5

# Test quit as non-start command, this time with --logtostderr. Test
# that the default logging level is WARNING, so that no INFO messages
# are printed between the marker and the (first line) error message
# from quit. Quit will error out because the server is already stopped.
send "echo marker; $argv quit --logtostderr\r"
eexpect "marker\r\nError"
eexpect ":/# "

# Now check that `--logtostderr` can override the default
send "$argv quit --logtostderr=INFO --vmodule=stopper=1\r"
eexpect "stop has been called"
eexpect ":/# "

send "exit\r"
eexpect eof
