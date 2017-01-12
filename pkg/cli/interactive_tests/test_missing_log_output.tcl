#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

# Check that a server started with only in-memory stores automatically
# logs to stderr.
send "$argv start --store=type=mem,size=1GiB\r"
eexpect "CockroachDB"
eexpect "starting cockroach node"

# Stop it.
interrupt
interrupt
eexpect ":/# "

# Check that a server started with --logtostderr
# logs even info messages to stderr.
send "$argv start --logtostderr\r"
eexpect "CockroachDB"
eexpect "starting cockroach node"

# Stop it.
interrupt
interrupt
eexpect ":/# "

# Check that --alsologtostderr can override the threshold
# regardless of what --logtostderr has set.
send "echo marker; $argv start --alsologtostderr=ERROR $silence_prop_eval_kv\r"
eexpect "marker\r\nCockroachDB node starting"

# Stop it.
interrupt
interrupt
eexpect ":/# "

send "echo marker; $argv start --alsologtostderr=ERROR --logtostderr $silence_prop_eval_kv\r"
eexpect "marker\r\nCockroachDB node starting"

# Stop it.
interrupt
interrupt
eexpect ":/# "

send "echo marker; $argv start --alsologtostderr=ERROR --logtostderr=false $silence_prop_eval_kv\r"
eexpect "marker\r\nCockroachDB node starting"

# Stop it.
interrupt
interrupt
eexpect ":/# "

# Start a regular server
send "$argv start >/dev/null 2>&1 & sleep 1\r"

# Now test `quit` as non-start command, and test that `quit` does not
# emit logging output between the point the command starts until it
# prints the final ok message.
send "echo marker; $argv quit $silence_prop_eval_kv\r"
set timeout 20
eexpect "marker\r\nok\r\n:/# "
set timeout 5

# Test quit as non-start command, this time with --logtostderr. Test
# that the default logging level is WARNING, so that no INFO messages
# are printed between the marker and the (first line) error message
# from quit. Quit will error out because the server is already stopped.
send "echo marker; $argv quit --logtostderr $silence_prop_eval_kv\r"
eexpect "marker\r\nError"
eexpect ":/# "

# Now check that `--alsologtostderr` can override the default
# properly, with or without `--logtostderr`.
send "$argv quit --alsologtostderr=INFO --logtostderr --vmodule=stopper=1\r"
eexpect "stop has been called"
eexpect ":/# "

send "$argv quit --alsologtostderr=INFO --logtostderr=false --vmodule=stopper=1\r"
eexpect "stop has been called"
eexpect ":/# "

send "$argv quit --alsologtostderr=INFO --vmodule=stopper=1\r"
eexpect "stop has been called"
eexpect ":/# "

send "exit\r"
eexpect eof
