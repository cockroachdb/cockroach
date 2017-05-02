#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that a server started with only in-memory stores and no --log-dir automatically logs to stderr."
send "$argv start --insecure --store=type=mem,size=1GiB\r"
eexpect "CockroachDB"
eexpect "starting cockroach node"
end_test

# Stop it.
interrupt
eexpect ":/# "

# Disable replication so as to avoid spurious purgatory errors.
start_server $argv
send "$argv zone set .default --disable-replication\r"
eexpect "num_replicas: 1"
eexpect ":/# "
stop_server $argv

start_test "Check that a server started with --logtostderr logs even info messages to stderr."
send "$argv start -s=path=logs/db --insecure --logtostderr\r"
eexpect "CockroachDB"
eexpect "starting cockroach node"
end_test

# Stop it.
interrupt
eexpect ":/# "

start_test "Check that --logtostderr can override the threshold but no error is printed on startup"
send "echo marker; $argv start -s=path=logs/db --insecure --logtostderr=ERROR 2>&1 | grep -v '^\\*'\r"
eexpect "marker\r\nCockroachDB node starting"
end_test

# Stop it.
interrupt
eexpect ":/# "

start_server $argv

start_test "Test that quit does not emit unwanted logging output"
# Unwanted: between the point the command starts until it
# either prints the final ok message or fails with some error
# (e.g. due to no definite answer from the server).
send "echo marker; $argv quit 2>&1 | grep '^\\(\[IWEF\]\[0-9\]\\)' \r"
set timeout 20
eexpect "marker\r\n:/# "
set timeout 5
end_test

start_test "Test that quit does not show INFO by defaults with --logtostderr"
# Test quit as non-start command, this time with --logtostderr. Test
# that the default logging level is WARNING, so that no INFO messages
# are printed between the marker and the (first line) error message
# from quit. Quit will error out because the server is already stopped.
send "echo marker; $argv quit --logtostderr\r"
eexpect "marker\r\nError"
eexpect ":/# "
end_test

start_test "Check that `--logtostderr` can override the default"
send "$argv quit --logtostderr=INFO --vmodule=stopper=1\r"
eexpect "stop has been called"
eexpect ":/# "
end_test

send "exit\r"
eexpect eof
