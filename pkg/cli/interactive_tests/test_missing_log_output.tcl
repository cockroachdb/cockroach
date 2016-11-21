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
send "\003"
sleep 1
send "\003"
eexpect ":/# "

# Start a regular server
send "$argv start >/dev/null 2>&1 & sleep 1\r"

# Now stop this server, and test that `quit` does not emit logging
# output between the point the command starts until it prints the
# final ok message.
send "echo marker; $argv quit\r"
set timeout 20
eexpect "marker\r\nok\r\n:/# "

send "exit\r"
eexpect eof
