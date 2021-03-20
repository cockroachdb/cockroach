#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# Set a larger timeout since we are talking to every node with a delay.
set timeout 90

start_test "Check --global flag runs as expected"

# Start a demo with --global set
spawn $argv demo --no-example-database --nodes 9 --global

# Ensure db is defaultdb.
eexpect "defaultdb>"

interrupt
eexpect eof
end_test
